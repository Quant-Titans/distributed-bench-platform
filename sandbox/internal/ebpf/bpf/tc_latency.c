// SPDX-License-Identifier: GPL-2.0
//
// tc_latency.c — eBPF TC hook for kernel-level RTT measurement.
//
// Attaches to the sandbox_net bridge (TC ingress + egress).
// On ingress (bot → sandbox): records ktime keyed by (saddr,daddr,sport,dport).
// On egress  (sandbox → bot): looks up the ingress timestamp, computes RTT,
//                             pushes a latency_event to a ring buffer.
//
// This bypasses Go runtime scheduler jitter and GC pauses — giving true
// kernel-level RTT that cannot be replicated by application-layer timing.

#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>
#include <bpf/bpf_core_read.h>

#define ETH_P_IP   0x0800
#define ETH_HLEN   14
#define IPPROTO_TCP 6
#define IPPROTO_UDP 17

// ── Data structures ───────────────────────────────────────────────────────────

struct flow_key {
    __u32 saddr;
    __u32 daddr;
    __u16 sport;
    __u16 dport;
    __u8  proto;
    __u8  _pad[3];
};

struct latency_event {
    __u32 saddr;
    __u32 daddr;
    __u16 sport;
    __u16 dport;
    __u8  proto;
    __u8  _pad[3];
    __u64 rtt_ns;         // kernel RTT: egress_ts - ingress_ts
    __u64 ingress_ns;     // absolute ingress kernel timestamp
    __u64 egress_ns;      // absolute egress  kernel timestamp
};

// ── Maps ─────────────────────────────────────────────────────────────────────

// Stores ingress timestamps keyed by flow; supports 64k concurrent flows.
struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, 65536);
    __type(key,   struct flow_key);
    __type(value, __u64);
} ingress_ts SEC(".maps");

// Ring buffer for userspace consumption (Go reads via cilium/ebpf).
struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 1 << 24);  // 16 MB
} latency_events SEC(".maps");

// ── Helpers ───────────────────────────────────────────────────────────────────

static __always_inline int parse_flow(struct __sk_buff *skb, struct flow_key *key) {
    void *data     = (void *)(long)skb->data;
    void *data_end = (void *)(long)skb->data_end;

    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end)
        return -1;
    if (eth->h_proto != bpf_htons(ETH_P_IP))
        return -1;

    struct iphdr *ip = data + ETH_HLEN;
    if ((void *)(ip + 1) > data_end)
        return -1;
    if (ip->protocol != IPPROTO_TCP && ip->protocol != IPPROTO_UDP)
        return -1;

    __u32 ip_hlen = ip->ihl * 4;
    void *transport = (void *)ip + ip_hlen;

    __u16 sport = 0, dport = 0;
    if (ip->protocol == IPPROTO_TCP) {
        struct tcphdr *tcp = transport;
        if ((void *)(tcp + 1) > data_end)
            return -1;
        sport = bpf_ntohs(tcp->source);
        dport = bpf_ntohs(tcp->dest);
    } else {
        struct udphdr *udp = transport;
        if ((void *)(udp + 1) > data_end)
            return -1;
        sport = bpf_ntohs(udp->source);
        dport = bpf_ntohs(udp->dest);
    }

    key->saddr = ip->saddr;
    key->daddr = ip->daddr;
    key->sport = sport;
    key->dport = dport;
    key->proto = ip->protocol;
    return 0;
}

// ── TC programs ───────────────────────────────────────────────────────────────

// Attached to TC ingress of sandbox_net bridge.
// Packet direction: bot → sandbox. Record arrival timestamp.
SEC("tc")
int tc_ingress(struct __sk_buff *skb) {
    struct flow_key key = {};
    if (parse_flow(skb, &key) < 0)
        return TC_ACT_OK;

    __u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&ingress_ts, &key, &ts, BPF_ANY);
    return TC_ACT_OK;
}

// Attached to TC egress of sandbox_net bridge.
// Packet direction: sandbox → bot. Look up ingress ts, compute RTT, emit event.
SEC("tc")
int tc_egress(struct __sk_buff *skb) {
    struct flow_key key = {};
    if (parse_flow(skb, &key) < 0)
        return TC_ACT_OK;

    // The response flow is the reverse of the request flow.
    struct flow_key rev_key = {
        .saddr = key.daddr,
        .daddr = key.saddr,
        .sport = key.dport,
        .dport = key.sport,
        .proto = key.proto,
    };

    __u64 *ingress_ns = bpf_map_lookup_elem(&ingress_ts, &rev_key);
    if (!ingress_ns)
        return TC_ACT_OK;

    __u64 now = bpf_ktime_get_ns();
    __u64 rtt = now - *ingress_ns;

    // Discard stale entries (> 5 seconds — indicates dropped requests)
    if (rtt > 5000000000ULL) {
        bpf_map_delete_elem(&ingress_ts, &rev_key);
        return TC_ACT_OK;
    }

    struct latency_event *ev = bpf_ringbuf_reserve(&latency_events, sizeof(*ev), 0);
    if (!ev)
        return TC_ACT_OK;

    ev->saddr      = rev_key.saddr;
    ev->daddr      = rev_key.daddr;
    ev->sport      = rev_key.sport;
    ev->dport      = rev_key.dport;
    ev->proto      = rev_key.proto;
    ev->rtt_ns     = rtt;
    ev->ingress_ns = *ingress_ns;
    ev->egress_ns  = now;

    bpf_ringbuf_submit(ev, 0);
    bpf_map_delete_elem(&ingress_ts, &rev_key);
    return TC_ACT_OK;
}

char __license[] SEC("license") = "GPL";
