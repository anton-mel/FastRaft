provider "aws" {
  region = "us-east-2"
  profile = "default"
}


# VPC
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  name    = "eks-vpc"
  cidr    = "10.0.0.0/16"
  azs     = ["us-east-2a", "us-east-2b", "us-east-2c"]
  public_subnets = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
    map_public_ip_on_launch = true

}

# EKS Cluster
module "eks" {
  source          = "terraform-aws-modules/eks/aws"
  cluster_name    = "raft-cluster"
  cluster_version = "1.30"
  vpc_id          = module.vpc.vpc_id
  subnet_ids     = module.vpc.public_subnets
  cluster_endpoint_public_access = true
  eks_managed_node_groups = {
    raft_nodes = {
      desired_size = 3
      max_size         = 5
      min_size         = 0
      instance_types = ["t2.micro"]
    }
    raftload_nodes = {
      desired_size = 1
      max_size     = 2
      min_size     = 1
      instance_types = ["t2.micro"]
      labels = {
        role = "raftload"
      }
    }
  }
}

data "aws_eks_cluster" "raft_cluster" {
  name = module.eks.cluster_name
  depends_on = [module.eks]
}

data "aws_eks_cluster_auth" "raft_cluster" {
  name = module.eks.cluster_name
  depends_on = [module.eks]
}

# ECR Repository
data "aws_ecr_repository" "raft_repo" {
  name = "raft"
}

provider "kubernetes" {
  host                   = data.aws_eks_cluster.raft_cluster.endpoint
  token                  = data.aws_eks_cluster_auth.raft_cluster.token
  cluster_ca_certificate = base64decode(data.aws_eks_cluster.raft_cluster.certificate_authority[0].data)
  config_path = "~/.kube/config"

   exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args = [
      "eks",
      "get-token",
      "--cluster-name",
      module.eks.cluster_name
    ]
  }

}



resource "kubernetes_config_map" "aws_auth" {
  metadata {
    name      = "aws-auth"
    namespace = "kube-system"
  }

  data = {
    mapUsers = yamlencode([
      {
        userarn = "arn:aws:iam::242201308548:root"
        username = "system:master"
        groups   = ["system:masters"]
      }
    ])
  }

  depends_on = [module.eks]
}

resource "kubernetes_cluster_role_binding" "admin" {
  metadata {
    name = "cluster-admin-binding"
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = "cluster-admin"
  }

  subject {
    kind      = "User"
    name      = "system:master"
    api_group = "rbac.authorization.k8s.io"
  }

  depends_on = [kubernetes_config_map.aws_auth]
}

resource "kubernetes_config_map" "raft_config" {
  metadata {
    name      = "raft-config"
  }

  data = {
    PEERS_IP = "raft-0.raft-service.default.svc.cluster.local:5000,raft-1.raft-service.default.svc.cluster.local:5000,raft-2.raft-service.default.svc.cluster.local:5000"
  }
}

resource "kubernetes_service" "raft_service" {
  metadata {
    name = "raft-service"
  }

  spec {
    cluster_ip = "None"
    selector = {
      app = "raft-node"
    }

    port {
      port        = 5000
      target_port = 5000
    }

    # type = "Headless"
  }
}

resource "kubernetes_deployment" "raftload" {
  metadata {
    name = "raftload"
    labels = {
      app = "raftload"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "raftload"
      }
    }

    template {
      metadata {
        labels = {
          app = "raftload"
        }
      }

      spec {
        node_selector = {
          role = "raftload"
        }

        container {
          name  = "raftload"
          image = "242201308548.dkr.ecr.us-east-2.amazonaws.com/raftload:latest"
          image_pull_policy = "Always"

          port {
            container_port = 8080
          }

          env {
            name = "SELF_IP"
            value_from {
              field_ref {
                field_path = "status.podIP"
              }
            }
          }

          env {
            name = "PEERS_IP"
            value_from {
              config_map_key_ref {
                name = kubernetes_config_map.raft_config.metadata[0].name
                key  = "PEERS_IP"
              }
            }
          }

        }
      }
    }
  }
}


resource "kubernetes_stateful_set" "raft" {
  metadata {
    name      = "raft"
  }

  spec {
    service_name = "raft-service"
    replicas     = 3

    selector {
      match_labels = {
        app = "raft-node"
      }
    }

    template {
      metadata {
        labels = {
          app = "raft-node"
        }
      }

      spec {
        affinity {
          pod_anti_affinity {
            required_during_scheduling_ignored_during_execution {
              label_selector {
                match_expressions {
                  key      = "app"
                  operator = "In"
                  values   = ["raft-node"]
                }
              }
              topology_key = "kubernetes.io/hostname"
            }
          }
        }

        container {
          name  = "raft-node"
          image = "${data.aws_ecr_repository.raft_repo.repository_url}:latest"
          image_pull_policy = "Always"

          port {
            container_port = 5000
          }

          env {
            name = "SELF_IP"
            value_from {
              field_ref {
                field_path = "status.podIP"
              }
            }
          }

          env {
            name = "PEERS_IP"
            value_from {
              config_map_key_ref {
                name = kubernetes_config_map.raft_config.metadata[0].name
                key  = "PEERS_IP"
              }
            }
          }

          
        }
      }
    }
  }
}


