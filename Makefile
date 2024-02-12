# Makefile for downloading Airbyte repository

# Variables
REPO_URL := https://github.com/airbytehq/airbyte.git
AIRBYTE_DIR := ./airbyte
SOCCERHUB_DIR := ./soccerhub


# Airbyte
airbyte-run-all:
	make airbyte-clone
	make airbyte-run

airbyte-clone:
	if [ -d "./airbyte" ]; then echo "Repository already exists"; else git clone ${REPO_URL}; fi

airbyte-run:
	$(AIRBYTE_DIR)/run-ab-platform.sh -b

airbyte-remove:
	rm -rf $(AIRBYTE_DIR)

airbyte-stop:
	docker-compose -f $(AIRBYTE_DIR)/docker-compose.yaml down

# Soccerhub
soccerhub-run-all:
	make soccerhub-run

soccerhub-run:
	docker-compose -f $(SOCCERHUB_DIR)/docker-compose.yaml -p soccerhub up -d

soccerhub-remove:
	rm -rf $(SOCCERHUB_DIR)/postgres/

soccerhub-stop:
	docker-compose -f $(SOCCERHUB_DIR)/docker-compose.yaml -p soccerhub down

