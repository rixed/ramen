<? include 'header.php' ?>

<h1>Does C++ devs worth more than web devs?</h1>
<p class="date">2019-07-31</p>

<h2>History, or why is the present the way it is, again?</h2>

<p>I've started programming professionally around the end of the 90s during the first internet bubble. Back then we had two secret weapons to bring customers what they needed much faster and for a fraction of the price than what was possible to the established practices of the established industry that we were disrupting:</p>

<center>GNU/Linux and web GUIs.</center>

<center><img src="blog/gnulinuxduo.png">&nbsp;<img height="276" src="blog/amazon.png"></center>

<p>The main reasons we used to put web GUI in front of everything, rather than the more traditional <a href="https://en.wikipedia.org/wiki/Fat_client">fat clients</a>, were several:</p>

<ul>
<li>It was a way to offer our services despite customers being equipped with a plethora of incompatible software and hardware, the most vendor closed of which still came with a web browser;</li>
<li>It was also one order of magnitude faster, thus also cheaper, to develop a dumb web front-end than a fat client;</li>
<li>Fat servers rather than fat clients made tests and deployments much simpler and robust.</li>
</ul>

<p>Essentially, the web was our battering ram against closed, expensive walled gardens of IBM, Sun, Novel, Bull and others which names have been forgotten.</p>

<p>But using tools and protocols designed to share arid papers across scientists was not without flaws though.</p>

<p>First and foremost, user interactions were limited to the bare minimum as the bag of tools available to web devs for user interactions were, at the time, severely limited. Fat clients using native or even java GUI libraries, could offer dedicated widgets to dedicated tasks, whereas to us everything had to be a web form.</p>

<p>Also, dumb clients had to perform a round trip to the distant server in between any two web forms which slowed things considerably. Even text interfaces, still around at the time, were often time preferred by their users for their responsiveness compared to our sluggish <em>ersatz</em>.</p>

<p>Finally, fat clients could make a much better use of client hardware, starting with the keyboard; Indeed, they had plenty of hotkeys and short cuts that web GUIs were missing which made them feel even slower. Fat clients could also store some data locally and work offline at least to some degree, or share data easily with other applications.</p>

<p>Still, then as always, the price argument fiercely prevailed.</p>

<p>The evolution of web GUI is well known:</p>

<p>Rather than acknowledging the fact that we needed a protocol to codify the semantic of a rich GUI and its universal client for each platform, the industry, unable by design to plan further away than the next baby step, carried on using the web for everything with many very minor improvements here and there to try to make it suck less while not loosing the price advantage.</p>

<p>Better standardisation of HTML slowly led to many new standards and revisions, most of them unused, and which, as the demise of XHTML have shown, were not allowed to break backward compatibility (that quickest road known to man toward overcomplexity).</p>

<p>Separation of graphical rendering from what to render, thanks to cascading style sheets, was invented to rival printed magazines for publishing eye-appealing documents on the web, rather than the customary arid scientific articles the embellishment of which climaxed with an horizontally &lt;center&gt;ed picture. We took advantage of CSS to make our web front ends suck less for the odd clients with unexpected screen sizes, the variety of which exploded when smartphones and then tablets became popular.</p>

<p>And most importantly javascript became prevalent. We initially used javascript to spare our clients from some of the round-trips to the server and patch the most infuriating UI corner cases.</p>

<p>At each of these baby steps we had to give back some of the initial advantages: our dumb clients became more complex, more expensive, less portable and less secure.</p>

<p>Fast forward to today. A whole industry grew like a cancer out of what was just a hack, and many computers only native application is now a web browser.</p>

<p>Finally, the initial flows of the web have been solved:</p>

<p>Today's web apps can use most of the local computer peripherals like touch screens, cameras and microphones. They can also store data locally and work offline. The round trips to the server are gone, replaced by the initial download of the application and the intermittent background discussion with the server.</p>

<p>Are the advantages still there, though?</p>

<p>If one dig deep enough into any web front-end today, regardless of how fashionable and on-the-edgeness of the stack, one is likely to still find the antique HTTP and HTML layer at the foundations. To achieve feature parity with fat clients, the web industry have had to pile up many different techs and standards basically simulating the native environment, the craziness and inadequation of which became the main jokes provider among professionals.</p>

<p>Has the time come, then, to revisit the tenet that web front-ends are more economic than fat clients?</p>

<h2>Questioning the custom</h2>

<p>Having to write a GUI for Ramen, I could initially though only of web front-ends and effectively have started two of them: one from scratch and one as a Grafana plugin, both of which I abandoned along the way as too costly to maintain and for lack of interest.  Indeed, I took the wrong decision to start from what users would most likely want to use them for (plotting dashboards) rather than what <em>I</em> would want to use it for (visualising Ramen's internals).</p>

<p>Rethinking the whole concept of a GUI, I also considered going native. So I looked for a cross platform UI library (therefore, some may argue, not properly native). What choices are there, as in summer 2019, of reasonably usable and portable GUI libraries?</p>

<p>My criterion were: Open-sourceness, portability, adequacy for drawing charts, easiness to link with other libraries (C or not), as I'd like to share some code with the OCaml server, at least in the beginning to speed up development.</p>

<ul>
<li>Gtk, the GNU Tool Kit, as anything GNU had to be considered first; It's C so easy to mix with anything else, but I couldn't come to peace with Gtk3 and the portability story is not stellar;</li>

<li>Qt is definitively portable and usable but I've avoided it so far because, apparently not happy enough with the intricacies of C++, its creators added a preprocessor and custom build system. Despite, it seemed also possible to link in non-qt libraries or even to have a non-qt entry point.</li>

<li>WxWidget, which reason d'être is portability, although which is actually less portable than Qt, but seems to lacks widgets to draw charts which I'd like to do at some point though;</li>

<li>Java, which gorgeous GUI I have always considered with both contempt and envy, is certainly the most portable option of all but linking with external libs and sharing data between OCaml and Java promised only blood and tears.</li>

<p>At first sight, Java and Qt seemed like the best options. I eventually picked Qt because Java would have felt going only half-way toward native and because I've been favorably impressed recently by a quick and lean app that happened to be done in Qt.</p>

<p>So I started and the ratio of C++ over OCaml lines of code quickly rose, as it takes many lines (and even files) to do anything in C++. It actually started to worry me as I wanted this endeavor to stay secret, never seriously mentioned it to anyone as I started to take this path, fearing to face mockery and other form of peer pressure from friends and coworkers to abandon what was deemed outdated technologies and get back in line with web UIs or better still no UI at all (this blog post being some kind of an outing).</p>

<p>My objective was first to demo a chart streamed end to end. This milestone was reached quick enough thanks to the <a href="https://www.qcustomplot.com">qcustomplot library</a> and I was satisfied enough that I started to develop the app further.</p>

<h2>First impressions</h2>

<p>What follows are my impressions, after just 2 months into this endeavor.</p>

<p>The first advantage of implementing a fat client over a web client is that it relieves us from having to make so many choices.</p>

<p>Shall a mere input line be needed in a web front-end, then myriads of components and libraries are on offer. To evaluate them one has to know not only what is needed now (a quick way to enter a text line!) but also what will be needed in the future. Will autocompletion be needed? A drop down list of suggestions? Real-time validators? Syntax highlighting even? Will it be possible to swap this library for another without too much trouble? Will this library be compatible with others I may use? What's its expected life span before it becomes deprecated?</p>

<p>Shall a mere input line needed in a Qt app, one just has to look up if it's called QLineInput or QInputLine (it's QLineEdit by the way), check the arguments of the default constructor, and leave everything else for later with the confidence that if a reasonable use case arises in the future for more sophistication regarding this widget, chances are it's going to be covered by some extra arguments or properties. And when, eventually, this input line does not cut it any longer, then there is always the solution to rewrite it, inherit from it, change it's painter, replace it with some external Qt library even, while being 100% certain it's not going to impact anything else outside of it.</p>

<p>The second thing I noticed while coding this beginning of a fat client, compared to programming a web front-end, is how rarely if ever one has to think about design.</p>

<p>Again, design decisions have been made already. A QLineEdit is just an input line like anyone expect (even trying to cater for various idioms on various client operating systems). Nobody expect much from it: focus works as expected, cursor moves as expected, copy and past works as expected, resizing works as expected, and it just is immediately recognizable for what it is: a simple line input widget, that, to be honest, nobody expects anything fancy from. A fat client is just, after all, a plain old boring tool with which one is supposed to do some work and then look away.</p>

<p>The expectations from web applications are very different and vary a lot from user to user, not in small part because the look and ergonomics are not standard, change significantly from on app to the next, and have also changed significantly over time. Also maybe because one has come to expect more fancy UI on the verge of entertainment from the web, in exchange for efficiency. Web is, after all, not really for work, is it?</p>

<p>Doing the equivalent of calling QLineEdit in the web would be to just write:</p>

<pre>
&lt;input type="text" value="What year is this?"/&gt;
</pre>

<p>but I suspect some modern web developers might not even recognize what that is.</p>

<p>Maintenance costs could also play in favor of fat clients these days: Despite Qt relying on C++, which is quite an unstable language, I believe many Qt programs written 20 years ago called their QLineEdit like I'm calling mine today and I expect Qt bits to rot slower than any of the web framework that's been released since I started to write this blog entry, some of which might be end-of-lined by the time I finish to write it.</p>

<p>It seems that C++ changing over time only allowed Qt to accept more syntax, for instance to connect signals and slots, and lower the amount of preprocessing it has to do, which is rather a nice thing (maybe one day all Qt custom keywords become NOPs and many of its standard types are replaced by C++ standard library ones once they catch up?). And the old ways to do it look very similar, still works today and gives the exact same result anyway.</p>

<p>So at the end of the day, does it take longer to write a fat client than a dumb client?  Has the web lost its price advantage?  Should C++ devs be paid more than web devs?</p>

<p>I'm still in the early process of writing and maintaining this app, but I feel that there will be no clear cut answer to this question.  And this simple fact is already revealing.</p>

<p>Certainly, the stack of technologies to learn is not wider in either one of the alternatives: Unix, C++, Qt and qmake does not look simpler or harder to master than any web stack in use today, and lasts longer. Debugging is actually simpler but bugs are more of a concern as fat clients are long-lived compared to most web front-ends, although this difference is also fading away slowly, as web devs are now also facing long running programs with memory leaking and performance degrading, long term data management, etc, with only the tools offered by the browser to diagnose and profile.</p>

<p>Portability is still probably slightly in favor of the web, but not by such a wide margin that we should care.</p>

<p>Deployment, though, is still easier on the web by a large margin. Although Qt tries to help with packaging a proper app on every platform, one still obviously has to have, at a bare minimum, a compilation box for Linux, Mac, Window, Android and iOS. Then comes the issue of distribution: users has to download and install the app; Only for Linux is this a solved problem since the 90s.</p>

<p>Compare this with the web, where users just have to point their browser to some URL; where you can easily and quickly deploy even special variants for special users, for instance with beta features or special bug traps.</p>

<p>So at this point I believe that from the supplier point of view, the cost of developing, maintaining and distributing an app is roughly of the same magnitude whether regardless of it being as a web front-end or as a fat client.</p>

<p>From the end users perspective, though, I have no data point yet. Will end users prefer the responsiveness and ergonomics of a boring fat client over the convenience of clicking a link to "install" an app and its upgrades, and merely reloading the page when it's stuck?</p>

<p>It might be nothing but rhetoric, mind you: nobody asked users if they preferred web apps in the 90s, and I bet they will have to adapt to whatever is more economic once again.</p>

<? include 'footer.php' ?>
