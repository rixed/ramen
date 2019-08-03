.SUFFIXES: .html .php .groff

PAGES = \
	index.html \
	download.html \
	build.html \
	tutorials.html \
	tutorials/monitoring_quick.html \
	design.html \
	requirements.html \
	programs.html \
	communication.html \
	archival.html \
	language_reference.html \
	glossary.html \
	man.html \
	roadmap.html \
	blog.html \
	blog/2018-12.html \
	blog/2019-01.html \
	blog/2019-02.html \
	blog/2019-03.html \
	blog/2019-05.html \
	blog/2019-07.html

MANPAGES = \
	man/ramen.html \
	man/archivist.html \
	man/compile.html \
	man/gc.html \
	man/httpd.html \
	man/tunneld.html \
	man/kill.html \
	man/links.html \
	man/alerter.html \
	man/notify.html \
	man/ps.html \
	man/replay.html \
	man/ringbuf.html \
	man/run.html \
	man/stats.html \
	man/supervisor.html \
	man/tail.html \
	man/test.html \
	man/timeseries.html \
	man/variants.html \
	man/confserver.html \
	man/compserver.html \
	man/choreographer.html \
	man/useradd.html \
	man/userdel.html \
	man/usermod.html

IMAGES = \
	tutorials/grafana.jpeg \
	menuicon.png \
	favicon.ico \
	blog/RamenVsKsql_cpu.svg \
	blog/RamenVsKsql_ram.svg \
	blog/RamenVsKsql_io.svg \
	blog/gnulinuxduo.png \
	blog/amazon.png

OTHERS = \
	blog/ksql.atop \
	blog/ramen.atop

all: $(PAGES) $(MANPAGES) $(IMAGES) $(OTHERS)

favicon.ico: icon.png
	@echo 'Building $@'
	@convert $< -resize 32x32 $@

menuicon.png: icon.png
	@echo 'Building $@'
	@convert $< -stroke '#efeae8' -colorize 100% $@

blog/RamenVsKsql_cpu.svg blog/RamenVsKsql_ram.svg blog/RamenVsKsql_io.svg: \
		blog/RamenVsKsql.gnuplot blog/RamenVsKsql.csv
	@echo 'Regenerating $@'
	@cd blog && gnuplot -c $(notdir $<)

man/ramen.groff: ../src/ramen
	@echo 'Extracting $@'
	@../src/ramen --help=groff > $@

man/supervisor.groff: ../src/ramen
	@echo 'Extracting $@'
	@../src/ramen supervisor --help=groff > $@

man/variants.groff: ../src/ramen
	@echo 'Extracting $@'
	@../src/ramen variants --help=groff > $@

man/confserver.groff: ../src/ramen
	@echo 'Extracting $@'
	@../src/ramen confserver --help=groff > $@

man/compserver.groff: ../src/ramen
	@echo 'Extracting $@'
	@../src/ramen compserver --help=groff > $@

man/choreographer.groff: ../src/ramen
	@echo 'Extracting $@'
	@../src/ramen choreographer --help=groff > $@

man/useradd.groff: ../src/ramen
	@echo 'Extracting $@'
	@../src/ramen useradd --help=groff > $@

man/userdel.groff: ../src/ramen
	@echo 'Extracting $@'
	@../src/ramen userdel --help=groff > $@

man/usermod.groff: ../src/ramen
	@echo 'Extracting $@'
	@../src/ramen usermod --help=groff > $@


man/test.groff: ../src/ramen
	@echo 'Extracting $@'
	@../src/ramen test --help=groff > $@

man/stats.groff: ../src/ramen
	@echo 'Extracting $@'
	@../src/ramen stats --help=groff > $@

man/ps.groff: ../src/ramen
	@echo 'Extracting $@'
	@../src/ramen ps --help=groff > $@

man/tail.groff: ../src/ramen
	@echo 'Extracting $@'
	@../src/ramen tail --help=groff > $@

man/replay.groff: ../src/ramen
	@echo 'Extracting $@'
	@../src/ramen replay --help=groff > $@

man/timeseries.groff: ../src/ramen
	@echo 'Extracting $@'
	@../src/ramen timeseries --help=groff > $@

man/notify.groff: ../src/ramen
	@echo 'Extracting $@'
	@../src/ramen notify --help=groff > $@

man/links.groff: ../src/ramen
	@echo 'Extracting $@'
	@../src/ramen links --help=groff > $@

man/run.groff: ../src/ramen
	@echo 'Extracting $@'
	@../src/ramen run --help=groff > $@

man/kill.groff: ../src/ramen
	@echo 'Extracting $@'
	@../src/ramen kill --help=groff > $@

man/httpd.groff: ../src/ramen
	@echo 'Extracting $@'
	@../src/ramen httpd --help=groff > $@

man/tunneld.groff: ../src/ramen
	@echo 'Extracting $@'
	@../src/ramen tunneld --help=groff > $@

man/compile.groff: ../src/ramen
	@echo 'Extracting $@'
	@../src/ramen compile --help=groff > $@

man/alerter.groff: ../src/ramen
	@echo 'Extracting $@'
	@../src/ramen alerter --help=groff > $@

man/gc.groff: ../src/ramen
	@echo 'Extracting $@'
	@../src/ramen gc --help=groff > $@

man/ringbuf.groff: ../src/ramen
	@echo 'Extracting $@'
	@../src/ramen ringbuf --help=groff > $@

man/archivist.groff: ../src/ramen
	@echo 'Extracting $@'
	@../src/ramen archivist --help=groff > $@

%.php: %.groff
	@echo 'Generating $@'
	@echo '<? include "header.php" ?>' > $@
	@groff -T html -P -l -P -r $< | \
	 sed -e '0,/<body>/d' -e '/<\/body>/,$$d' >> $@
	@echo '<? include "footer.php" ?>' >> $@

%.html: %.php header.php footer.php data.php
	@echo 'Expanding HTML file $@'
	@php -n $< > $@ || true
	@if grep Warning $@ || grep 'Parse error:' $@ ; then \
	  rm $@ ;\
	 fi

doc_dir ?= $(PWD)/../../ramen-docs

install: $(PAGES) $(MANPAGES) $(IMAGES) $(OTHERS) style.css
	@echo 'Installing pages into $(prefix)$(doc_dir)'
	@install -d '$(prefix)$(doc_dir)'
	for f in $^ ; do \
	   mkdir -p "$(DESTDIR)$(doc_dir)/$$(dirname $$f)" ;\
	   cp "$$f" "$(DESTDIR)$(doc_dir)/$$(dirname $$f)" ;\
	 done

clean:
	@$(RM) $(PAGES) $(MANPAGES)
	@$(RM) $(MANPAGES:.html=.groff) $(MANPAGES:.html=.php)
