output "cluster_endpoint" {
  value       = module.eks.cluster_endpoint
  description = "EKS API server endpoint"
}

output "cluster_name" {
  value       = module.eks.cluster_name
  description = "EKS cluster name"
}

output "kubeconfig_command" {
  value       = "aws eks update-kubeconfig --name ${module.eks.cluster_name} --region ${var.aws_region}"
  description = "Run this after terraform apply to configure kubectl"
}

output "leaderboard_url" {
  value       = "kubectl get svc leaderboard -n ${var.k8s_namespace} -o jsonpath='{.status.loadBalancer.ingress[0].hostname}'"
  description = "Run this to get the live leaderboard URL"
}

output "platform_status" {
  value       = "kubectl get pods -n ${var.k8s_namespace}"
  description = "Check all pod health after deploy"
}
