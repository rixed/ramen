<? include 'header.php' ?>
<h1>Tutorials</h1>

<p>Few documentations about how to achieve basic things, to help start with Ramen</p>

<ul>
<? foreach ($info_pages['tutorials.html']['sub_pages'] as $page => $p) { ?>
<li><a href="tutorials/<?=$page?>"><?=$p['title']?></a></li>
<? } ?>
</ul>

<? include 'footer.php' ?>
