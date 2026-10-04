PY ?= python3

.PHONY: install demo download simulate test report clean

install:
	$(PY) -m pip install -r requirements.txt

# the one command: download the window (Nov 1-28, ~14 GB), reset, simulate with all scenarios, write outputs/
demo:
	$(PY) -m ninebar demo

download:
	$(PY) -m ninebar download

simulate:
	$(PY) -m ninebar reset && $(PY) -m ninebar simulate

test:
	$(PY) -m pytest -q

report:
	$(PY) -m ninebar report

clean:
	$(PY) -m ninebar reset
