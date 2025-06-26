import google.cloud.logging
import logging

def write_to_cloud_logging(message, log_name='my_python_log', severity='INFO', **labels):
    """Writes a log message to Google Cloud Logging with custom labels.

    Args:
        message: The log message text.
        log_name (optional): The name of the Cloud Logging log to write to. Defaults to 'my_python_log'.
        severity (optional): The severity level of the log message. Defaults to 'INFO'.
                             Possible values: 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'.
        **labels: Additional keyword arguments representing custom labels (key-value pairs).
    """

    # Initialize the Cloud Logging client
    logging_client = google.cloud.logging.Client(project=projectID)
    logger = logging_client.logger(log_name)

    # Create a LogEntry object
    log_entry = google.cloud.logging.entries.LogEntry(
        payload=message,
        severity=severity,
        logger=logger,
        labels=labels
    )

    # Write the log entry to Cloud Logging
    logger.log_struct(log_entry.to_api_repr())

# Example usage
write_to_cloud_logging("Hello from Cloud Logging!", severity='INFO', environment='production', user='admin')
write_to_cloud_logging("This is a warning message.", severity='WARNING', component='database', error_code='404')