<? include 'header.php' ?>
<h1>Building from sources</h1>

<p>It is possible to build and run Ramen from sources both from a Linux or a MacOS machine (although it's easier from Linux).</p>

<p>First, you need to get those sources:</p>

<pre>
$ git clone https://github.com/rixed/ramen.git
$ cd ramen
</pre>

<p>Ramen is implemented in OCaml and C so you need recent compilers for those languages. You also need gnumake.</p>

<p>To install the few libraries Ramen depends on, the easiest option is probably to use the <a href="https://opam.ocaml.org">opam</a> package manager. With it, you could merely:</p>

<pre>
$ opam repo add --priority=1 ramen git://github.com/rixed/ocalme-opam-repository.git
$ opam pin --no-action add ramen $PWD
$ opam install --deps-only ramen
</pre>

<p>You can then compile "by hand" running:</p>

<pre>
$ ./configure
$ make
$ make install
</pre>

<p>or let opam do it:</p>

<pre>
$ opam install ramen
</pre>

<p>Then if you modify the source code and want to reinstall you have to either <code>make reinstall</code> or <code>opam upgrade ramen</code>, depending on the method you have chosen to install ramen.</code></p>

<p>The result of the compilation should be a single executable file named <code>ramen</code> and a directory named <code>bundle</code> containing all the libraries that are needed at runtime to compile and run the data stream workers. This directory can be copied anywhere as long as <code>ramen</code> can find it using the <code>RAMEN_BUNDLE_DIR</code> environment variable.</p>

<? include 'footer.php' ?>
