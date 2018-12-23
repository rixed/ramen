<? include 'header.php' ?>
<h1>Blog posts</h1>

<p>Some blog posts on specific features as they are being worked on. Might helps understand the trade offs, design decisions and blunders.</p>

<ul>
<? foreach ($info_pages['blog.html']['sub_pages'] as $page => $p) { ?>
<li><a href="<?=$page?>"><?=$p['date']?></a>: <?=$p['title']?></li>
<? } ?>
</ul>

<? include 'footer.php' ?>
