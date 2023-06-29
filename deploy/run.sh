gcloud dataflow jobs run ${JOB_NAME} \
  --gcs-location ${TEMPLATE_LOCATION} \
  --parameters test=test