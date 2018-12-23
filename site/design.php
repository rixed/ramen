<? include 'header.php' ?>
<h1>Basic requirements and design overview</h1>

<ul>
<? foreach ($info_pages['design.html']['sub_pages'] as $page => $p) { ?>
<li><a href="<?=$page?>"><?=$p['title']?></a>;</li>
<? } ?>
</ul>

<? include 'footer.php' ?>
