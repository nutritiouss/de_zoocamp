terraform {
  required_version = ">= 1.0"
  backend "local" {}  # Can change from "local" to "gcs" (for google) or "s3" (for aws), if you would like to preserve your tf-state online
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.project
  region = var.region
}

# Data Lake Bucket
resource "google_storage_bucket" "data-lake-bucket" {
  name          = "${local.data_lake_bucket}_${var.project}" # Concatenating DL bucket & Project name for unique naming
  location      = var.region

  # Optional, but recommended settings:
  storage_class = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  force_destroy = true
}


# Data Warehouse
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.BQ_DATASET
  project    = var.project
  location   = var.region
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "prod_dataset" {
  dataset_id = var.dbt_core_dataset
  project    = var.project
  location   = var.region
  delete_contents_on_destroy = true
}
#
# VM instance
resource "google_compute_instance" "vm_instance" {
  name          = "airflow-instance"
  project       = var.project
  machine_type  = "e2-standard-4"
  zone          = "europe-west2-a"
  tags = ["http-server","https-server"]


  boot_disk {
    initialize_params {
      image = var.vm_image
    }
  }


 network_interface {
    network = google_compute_network.default.name

    access_config {
      // Ephemeral public IP
    }
  }
}

resource "google_compute_firewall" "default" {
  name    = "test-firewall"
  network = google_compute_network.default.name

  allow {
    protocol = "icmp"
  }

  allow {
    protocol = "tcp"
    ports    = ["80", "8080", "3000"]
  }

  source_tags = ["web"]
}

resource "google_compute_network" "default" {
  name = "test-network"
}










#
# resource "google_compute_firewall" "ssh" {
#   name = "allow-ssh"
#   allow {
#     ports    = ["22"]
#     protocol = "tcp"
#   }
#   direction     = "INGRESS"
#   network       = "default"
#   priority      = 1000
#   source_ranges = ["0.0.0.0/0"]
#   target_tags   = ["allow-ssh"]
# }
#
# resource "google_compute_firewall" "http-server" {
#   name    = "allow-web"
#   network = "default"
#
#   allow {
#     protocol = "tcp"
#     ports    = ["80"]
#   }
#
#   allow {
#     protocol = "tcp"
#     ports    = ["443"]
#   }
#
#   // Allow traffic from everywhere to instances with an http-server tag
#   source_ranges = ["0.0.0.0/0"]
#   target_tags   = ["allow-web"]
# }