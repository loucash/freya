PROJECT := freya
SNAME := $(PROJECT)

ERL := erl
EPATH = -pa ebin -pz deps/*/ebin
TEST_EPATH = -pz deps/*/ebin -I deps/proper/include -pa ebin -pa test
PLT_APPS = $(shell ls $(ERL_LIB_DIR) | grep -v interface | sed -e 's/-[0-9.]*//')
DIALYZER_OPTS= -Wno_undefined_callbacks --fullpath
CQLSH = cqlsh

.PHONY: all build_plt compile configure console deps doc clean depclean distclean dialyze release telstart test test-console

all: deps compile cassandra-freya kairosdb-ui

build_plt:
	@dialyzer --build_plt --apps $(PLT_APPS)

compile:
	@./rebar compile

compile-fast:
	@./rebar skip_deps=true compile

configure:
	@./rebar get-deps compile

console:
	$(ERL) -sname $(PROJECT) $(EPATH)

deps:
	@./rebar get-deps

doc:
	@./rebar skip_deps=true doc

clean:
	@./rebar skip_deps=true clean

stress: compile
	@erlc -o ./stress ./stress/stress.erl
	@$(ERL) -sname $(PROJECT) $(EPATH) -pa stress

depclean:
	@./rebar clean

distclean:
	@./rebar clean delete-deps
	@rm -rf logs

dialyze:
	@dialyzer $(DIALYZER_OPTS) -r ebin

start:
	$(ERL) -sname $(PROJECT) $(EPATH) -s $(PROJECT)

test: compile-fast cassandra-freya
	@./rebar skip_deps=true ct verbose=1

test-console: test-compile
	@erlc $(TEST_EPATH) -o test test/*.erl
	$(ERL) -sname $(PROJECT)_test  $(TEST_EPATH) -config sys

cassandra-freya:
	$(CQLSH) < ./priv/schema.cql

dev: compile-fast dev-console

dev-clean: cassandra-freya dev

dev-console:
	$(ERL) -sname $(SNAME) $(EPATH) -s freya -config sys

spam:
	@erl -pa deps/*/ebin -pa ebin -config sys -s lager

kairosdb-ui: priv/ui

priv/ui:
	@cd priv && curl -O http://mtod.org/ui.tar.gz && tar xzfv ui.tar.gz

ct-single:
	@mkdir -p ct_log
	@if [ -z "$(suite)" ] || [ -z "$(case)" ]; then \
	    echo "Provide args. e.g. suite=freya_io case=t_tcp_write"; exit 1; \
	    else true; fi
	@ct_run -noshell -pa deps/*/ebin -pa ebin -include include -include src \
	    -include deps/*/include -sname freya_test -logdir ct_log -ct_hooks cth_surefire \
		-sasl sasl_error_logger false \
	    -dir $(dir $(shell find . -name $(suite)_SUITE.erl)) -suite $(suite)_SUITE -case $(case)
	@open ct_log/all_runs.html
