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
#firewall for VM instance
resource "google_compute_firewall" "allow_http" {
  name    = "allow-http-rule"
  network = "default"
  allow {
    ports    = ["8080","3000"]
    protocol = "tcp"
  }
  target_tags = ["allow-http"]
   source_ranges = ["0.0.0.0/0"]
  priority    = 1000

}

# VM instance
resource "google_compute_instance" "vm_instance" {
  name          = "airflow-instance"
  project       = var.project
  machine_type  = "e2-standard-4"
  zone          = "europe-west2-a"
  tags = ["allow-http"]

  boot_disk {
    initialize_params {
      image = var.vm_image
    }
  }

 network_interface {
    network = "default"
    access_config {
      // Ephemeral public IP
    }
  }


}

