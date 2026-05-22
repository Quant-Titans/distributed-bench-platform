/* Minimal vmlinux.h for eBPF compilation.
 * In production, generate with: bpftool btf dump file /sys/kernel/btf/vmlinux format c > vmlinux.h
 * This minimal version covers all types used in tc_latency.c.
 */
#pragma once

typedef unsigned char      __u8;
typedef unsigned short     __u16;
typedef unsigned int       __u32;
typedef unsigned long long __u64;
typedef signed char        __s8;
typedef signed short       __s16;
typedef signed int         __s32;
typedef signed long long   __s64;

#define TC_ACT_OK    0
#define TC_ACT_SHOT  2

struct ethhdr {
    unsigned char  h_dest[6];
    unsigned char  h_source[6];
    __u16          h_proto;
} __attribute__((packed));

struct iphdr {
    __u8  ihl:4;
    __u8  version:4;
    __u8  tos;
    __u16 tot_len;
    __u16 id;
    __u16 frag_off;
    __u8  ttl;
    __u8  protocol;
    __u16 check;
    __u32 saddr;
    __u32 daddr;
} __attribute__((packed));

struct tcphdr {
    __u16 source;
    __u16 dest;
    __u32 seq;
    __u32 ack_seq;
    __u16 res1:4, doff:4, fin:1, syn:1, rst:1, psh:1, ack:1, urg:1, ece:1, cwr:1;
    __u16 window;
    __u16 check;
    __u16 urg_ptr;
} __attribute__((packed));

struct udphdr {
    __u16 source;
    __u16 dest;
    __u16 len;
    __u16 check;
} __attribute__((packed));

struct __sk_buff {
    __u32 len;
    __u32 pkt_type;
    __u32 mark;
    __u32 queue_mapping;
    __u32 protocol;
    __u32 vlan_present;
    __u32 vlan_tci;
    __u32 vlan_proto;
    __u32 priority;
    __u32 ingress_ifindex;
    __u32 ifindex;
    __u32 tc_index;
    __u32 cb[5];
    __u32 hash;
    __u32 tc_classid;
    __u32 data;
    __u32 data_end;
    __u32 napi_id;
    __u32 family;
    __u32 remote_ip4;
    __u32 local_ip4;
    __u32 remote_ip6[4];
    __u32 local_ip6[4];
    __u32 remote_port;
    __u32 local_port;
    __u32 data_meta;
    __u64 tstamp;
    __u32 wire_len;
    __u32 gso_segs;
    __u64 sk;
    __u32 gso_size;
    __u8  tstamp_type;
    __u32 hwtstamp;
};

/* UAPI network typedefs — used by bpf_helper_defs.h */
typedef __u16 __be16;
typedef __u32 __be32;
typedef __u64 __be64;
typedef __u32 __wsum;
typedef __u16 __le16;
typedef __u32 __le32;

/* BPF map types — subset needed by tc_latency.c */
#define BPF_MAP_TYPE_LRU_HASH   9
#define BPF_MAP_TYPE_RINGBUF    27

/* BPF map update flags */
#define BPF_ANY     0
#define BPF_NOEXIST 1
#define BPF_EXIST   2

/* Compiler attributes */
#define __always_inline inline __attribute__((always_inline))
#define __noinline      __attribute__((noinline))
