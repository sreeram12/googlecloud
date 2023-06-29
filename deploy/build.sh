gcloud builds submit \
  --project=${PROJECT} \
  --tag ${IMAGE_NAME} \

gcloud dataflow flex-template build ${TEMPLATE_LOCATION} \
  --image ${IMAGE_NAME} \
  --sdk-language PYTHON \
  --metadata-file deploy/metadata.json