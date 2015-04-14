PROJECT := freya
REBAR := ./rebar
SNAME := $(PROJECT)

ERL := erl
EPATH = -pa ebin -pz deps/*/ebin
TEST_EPATH = -pz deps/*/ebin -I deps/proper/include -pa ebin -pa test
PLT_APPS = $(shell ls $(ERL_LIB_DIR) | grep -v interface | sed -e 's/-[0-9.]*//')
DIALYZER_OPTS= -Wno_undefined_callbacks --fullpath
CQLSH = cqlsh

.PHONY: all build_plt compile configure console deps doc clean depclean distclean dialyze release test test-console

all: deps compile data/ring cassandra-freya

build_plt:
	@dialyzer --build_plt --apps $(PLT_APPS)

compile:
	$(REBAR) compile

compile-fast:
	$(REBAR) skip_deps=true compile

configure:
	$(REBAR) get-deps compile

console: data/ring
	$(ERL) -sname $(PROJECT) $(EPATH) -config riak_core

deps:
	$(REBAR) get-deps

doc:
	$(REBAR) skip_deps=true doc

clean:
	$(REBAR) skip_deps=true clean

depclean:
	$(REBAR) clean

distclean:
	$(REBAR) clean delete-deps
	@rm -rf logs
	@rm -rf ct_log
	@rm -rf log
	@rm -rf _build

dialyze:
	@dialyzer $(DIALYZER_OPTS) -r ebin

dialyze/dialyzer_plt:
	mkdir -p dialyze
	curl -L "https://github.com/esl/erlang-plts/blob/master/plts/travis-erlang-r16b02.plt?raw=true" -o dialyze/dialyzer_plt
	cp dialyze/dialyzer_plt /home/travis/.dialyzer_plt

/home/travis/.dialyzer_plt:
	cp dialyze/dialyzer_plt /home/travis/.dialyzer_plt

dialyzer-travis: dialyze/dialyzer_plt /home/travis/.dialyzer_plt
	@dialyzer $(DIALYZER_OPTS) -r ebin

start:
	$(ERL) -sname $(PROJECT) $(EPATH) -s $(PROJECT)

test: compile-fast cassandra-freya
	$(REBAR) -C rebar.test.config get-deps compile
	$(REBAR) -C rebar.test.config ct skip_deps=true

dist-test: devclean _build/devrels
	rebar -C rebar.dist.config skip_deps=true compile
	./riak_test -v -c freya -d dist-tests -F riak_test.config

single-dist-test:
	rebar -C rebar.dist.config skip_deps=true compile
	./riak_test -v -c freya -t $(test) -F riak_test.config

test-console: test-compile
	@erlc $(TEST_EPATH) -o test test/*.erl
	$(ERL) -sname $(PROJECT)_test  $(TEST_EPATH) -config sys

cassandra-freya:
	$(CQLSH) < ./priv/schema.cql

frontend: priv/build

priv/build:
	@cd priv/frontend && component build

dev: compile-fast dev-console

dev-clean: cassandra-freya dev

dev-console: compile-fast
	$(ERL) -sname $(SNAME) $(EPATH) -s freya -config sys -config riak_core

spam:
	@erl -pa deps/*/ebin -pa ebin -config sys -s lager

data/ring:
	@rm -rf data/ring
	@mkdir -p data/ring

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

rel: all
	@./relx -c rel/freya.config -o _build/release --overlay_vars=vars/deploy.config

release: deps compile data/ring
	@./relx -c rel/freya.config -o _build/release --overlay_vars=vars/deploy.config

relclean:
	@rm -rf rel/freya

_build/devrels: devclean dev1 dev2 dev3 dev4
	@cd _build/devrels && git init && git add -f dev* && git commit -m "initial"

dev1 dev2 dev3 dev4:
	./relx -c rel/freya.config -o _build/devrels/$@ --overlay_vars=vars/$@.config

devclean:
	rm -rf _build/devrels
