provider "google-beta" {
  credentials = file("<replace_file_here>")
  project     = var.project
  region      = "us-central1"
}


