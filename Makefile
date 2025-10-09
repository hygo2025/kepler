PYTHON_VERSION = 3.9
VENV_DIR = .venv
REQUIREMENTS_FILE = requirements.txt
ACTIVATE = . $(VENV_DIR)/bin/activate

PYTHONPATH ?= $(PWD)

export PYTHONUNBUFFERED = 1
export PYTHONPATH

check_python_version:
	@installed_version=$$(pyenv versions --bare | grep $(PYTHON_VERSION)); \
	if [ -z "$$installed_version" ]; then \
		echo "Python version $(PYTHON_VERSION) not found. Installing..."; \
		pyenv install $(PYTHON_VERSION); \
		pyenv global $(PYTHON_VERSION); \
	else \
		echo "Python version $(PYTHON_VERSION) is already installed."; \
	fi

$(VENV_DIR): check_python_version
	@echo "Creating virtual environment in $(VENV_DIR)..."
	pyenv local $(PYTHON_VERSION)
	pyenv exec python -m venv $(VENV_DIR)
	$(ACTIVATE) && pip install --upgrade pip && pip install -r $(REQUIREMENTS_FILE)

install: $(VENV_DIR)

update:
	$(ACTIVATE) && pip install --upgrade -r $(REQUIREMENTS_FILE)

clean:
	rm -rf $(VENV_DIR)
	rm -rf recommendations

remove: clean
	rm -f $(REQUIREMENTS_FILE)

.PHONY: prepare_data #01
prepare_data:
	export $(shell cat .env) && $(VENV_DIR)/bin/python src/main.py -o prepare_data

.PHONY: make_user_sessions 02
make_user_sessions:
	export $(shell cat .env) && $(VENV_DIR)/bin/python src/main.py -o make_user_sessions

.PHONY: enrich_events 03
enrich_events:
	export $(shell cat .env) && $(VENV_DIR)/bin/python src/main.py -o enrich_events

.PHONY: rnn_4rec
rnn_4rec:
	export $(shell cat .env) && $(VENV_DIR)/bin/python src/main.py -m RNN4Rec

.PHONY: install update clean remove
