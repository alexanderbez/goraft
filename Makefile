.PHONY: lint

NO_COLOR=\x1b[0m
CONTEXT_COLOR=\x1b[94;01m
ERR_COLOR=\x1b[91;01m

lint:
	@echo "$(CONTEXT_COLOR)running gometalinter...$(NO_COLOR)"
	@gometalinter ./... || (echo "$(ERR_COLOR)gometalinter failed!$(NO_COLOR)"; exit 1)