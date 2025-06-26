provider "google" {
  credentials = file("credential.json")
  project     = "<project-here_>"
  region      = "us-central1"
}


resource "google_compute_network" "dataproc_network" {
  name                    = "basic-cluster-network"
  auto_create_subnetworks = true
}

resource "google_compute_firewall" "firewall_rules" {
  name    = "basic-cluster-firewall-rules"
  network = google_compute_network.dataproc_network.name

  // Allow ping
  allow {
    protocol = "icmp"
  }
  //Allow all TCP ports
  allow {
    protocol = "tcp"
    ports    = ["1-65535"]
  }
  //Allow all UDP ports
  allow {
    protocol = "udp"
    ports    = ["1-65535"]
  }
  source_ranges = ["0.0.0.0/0"]
}


resource "google_dataproc_cluster" "mycluster" {
  provider = "google"
  name     = "basiccluster"
  region   = "us-central1"

  cluster_config {
  gce_cluster_config {
    network = ""
  }
    master_config {
      num_instances     = 1
      machine_type      = "n1-standard-4"
    }

    worker_config {
      num_instances     = 2
      machine_type      = "n1-standard-8"


    }


  }



}
