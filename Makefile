.PHONY: lint

NO_COLOR=\x1b[0m
PROMPT_COLOR=\x1b[94;01m
ERR_COLOR=\x1b[91;01m

lint:
	@echo "$(PROMPT_COLOR)running gometalinter...$(NO_COLOR)"
	@gometalinter ./...