# Makefile for downloading Airbyte repository

# Variables
REPO_URL := https://github.com/airbytehq/airbyte.git
LOCAL_DIR := ./airbyte
FOLDER := ./airbyte

airbyte-run-all:
	make airbyte-clone
	make airbyte-run

airbyte-clone:
	if [ -d "./airbyte" ]; then echo "Repository already exists"; else git clone ${REPO_URL}; fi

airbyte-run:
	$(LOCAL_DIR)/run-ab-platform.sh -b

airbyte-remove:
	rm -rf $(LOCAL_DIR)

airbyte-stop:
	docker-compose -f $(LOCAL_DIR)/docker-compose.yaml down

