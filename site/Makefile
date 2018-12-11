.SUFFIXES: .html .mustache .groff

PAGES = \
	index.html \
	download.html \
	build.html \
	tutorials.html \
	tutorials/host_monitoring.html \
	tutorials/detecting_scans.html \
	design.html \
	programs.html \
	communication.html \
	archival.html \
	language_reference.html \
	glossary.html \
	man.html \
	roadmap.html \
	blog.html \
	blog/2018-12.html

MANPAGES = \
	man/ramen.html \
	man/archivist.html \
	man/compile.html \
	man/gc.html \
	man/httpd.html \
	man/kill.html \
	man/links.html \
	man/notifier.html \
	man/notify.html \
	man/ps.html \
	man/replay.html \
	man/ringbuf.html \
	man/run.html \
	man/stats.html \
	man/supervisor.html \
	man/tail.html \
	man/test.html \
	man/timerange.html \
	man/timeseries.html \
	man/variants.html

all: $(PAGES) $(MANPAGES)

man/ramen.groff: ../src/ramen
	@echo "Extracting $@"
	@../src/ramen --help=groff > $@

man/supervisor.groff:
	@echo "Extracting $@"
	@../src/ramen supervisor --help=groff > $@

man/variants.groff:
	@echo "Extracting $@"
	@../src/ramen variants --help=groff > $@

man/test.groff:
	@echo "Extracting $@"
	@../src/ramen test --help=groff > $@

man/timerange.groff:
	@echo "Extracting $@"
	@../src/ramen timerange --help=groff > $@

man/stats.groff:
	@echo "Extracting $@"
	@../src/ramen stats --help=groff > $@

man/ps.groff:
	@echo "Extracting $@"
	@../src/ramen ps --help=groff > $@

man/tail.groff:
	@echo "Extracting $@"
	@../src/ramen tail --help=groff > $@

man/replay.groff:
	@echo "Extracting $@"
	@../src/ramen replay --help=groff > $@

man/timeseries.groff:
	@echo "Extracting $@"
	@../src/ramen timeseries --help=groff > $@

man/notify.groff:
	@echo "Extracting $@"
	@../src/ramen notify --help=groff > $@

man/links.groff:
	@echo "Extracting $@"
	@../src/ramen links --help=groff > $@

man/run.groff:
	@echo "Extracting $@"
	@../src/ramen run --help=groff > $@

man/kill.groff:
	@echo "Extracting $@"
	@../src/ramen kill --help=groff > $@

man/httpd.groff:
	@echo "Extracting $@"
	@../src/ramen httpd --help=groff > $@

man/compile.groff:
	@echo "Extracting $@"
	@../src/ramen compile --help=groff > $@

man/notifier.groff:
	@echo "Extracting $@"
	@../src/ramen notifier --help=groff > $@

man/gc.groff:
	@echo "Extracting $@"
	@../src/ramen gc --help=groff > $@

man/ringbuf.groff:
	@echo "Extracting $@"
	@../src/ramen ringbuf --help=groff > $@

man/archivist.groff:
	@echo "Extracting $@"
	@../src/ramen archivist --help=groff > $@

%.mustache: %.groff
	@echo "Generating $@"
	@echo '{{>header}}' > $@
	@groff -T html -P -l -P -r $< | \
	 sed -e '0,/<body>/d' -e '/<\/body>/,$$d' >> $@
	@echo '{{>footer}}' >> $@

%.html: %.mustache header.mustache footer.mustache data.yml
	@echo 'Expanding HTML file $@'
	@mustache data.yml $< > $@

doc_dir ?= $(PWD)/../../ramen-docs

install: $(PAGES) $(MANPAGES) style.css
	@echo "Installing pages into $(prefix)$(doc_dir)"
	@install -d "$(prefix)$(doc_dir)"
	for f in $^ ; do \
	   mkdir -p "$(DESTDIR)$(doc_dir)/$$(dirname $$f)" ;\
	   cp "$$f" "$(DESTDIR)$(doc_dir)/$$(dirname $$f)" ;\
	 done

clean:
	@$(RM) $(PAGES)
