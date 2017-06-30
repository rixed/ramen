function select_node(node, set_node)
{
  d3.selectAll('svg#graph g.node.selected').classed('selected', false);
  if (node != null) {
    d3.select('svg#graph g#node_'+node.name).classed('selected', true);
    set_node(node.name);
  } else {
    set_node(null);
  }
}

// Who do not like globals?
var node_of_name, svg, node, link, width, height, top_nodes, simulation, nodes, links;

function init()
{
  console.log('initializing...');
  svg = d3.select('svg#graph');
  node = svg.selectAll('g.node');
  link = svg.selectAll('g.link');
  nodes = [];
  links = [];
  top_nodes = [];
  var svgbox = document.getElementById('graph').getBoundingClientRect();
  width = svgbox.width;
  height = svgbox.height;
  console.log('width='+width+', height='+height);

  simulation = d3.forceSimulation(nodes)
      .force('center', d3.forceCenter(width/2, height/2))
      .force('positioning', d3.forceX(width/2).strength(function(node) { return top_nodes[node.name] ? 1:0.2; }))
      .force('positioning', d3.forceY(100).strength(function(node) { return top_nodes[node.name] ? 1:0; }))
      .force('link', d3.forceLink(links).id(function(node) { return node.name; }).distance(100))
      .force('charge', d3.forceManyBody().strength(-100))
      .force('collide', d3.forceCollide().radius(function(node) { return node.radius * 1.2; }))
      .alphaTarget(1)
      .on('tick', update_positions);
}

function node_label(d) { return d.name; }
function node_class(d) { return 'node ' + d.type_of_operation.replace(/ /g, '_'); }
function link_name(link) { return link[0] +'_'+ link[1]; }

function update_positions()
{
  console.log('updating positions...');
  node.select('text')
      .text(node_label)
      .attr('x', function (node) { return node.x; })
      .attr('y', function (node) { return node.y; });
  node.select('circle')
      .attr('cx', function (node) { return node.x; })
      .attr('cy', function (node) { return node.y; });

  // Return [x,y] of the point on [(x1,y1), (x2,y2)] at dist from (x2,y2):
  function chop_line(x0, y0, x1, y1, dist) {
    var dx = x1 - x0;
    var dy = y1 - y0;
    var l = Math.sqrt(dx*dx + dy*dy);
    if (l <= dist) { return [x0, y0]; }
    var alpha = Math.atan2 (dy, dx);
    var ll = l - dist;
    var x = x0 + (ll * Math.cos(alpha));
    var y = y0 + (ll * Math.sin(alpha));
    return [isNaN(x) ? x0:x, isNaN(y) ? y0:y ];
  }
  var arrow_length = 10;
  link.select('line')
      .attr('x1', function (link) {
        var n0 = node_of_name[link[0]];
        var n1 = node_of_name[link[1]];
        var start = chop_line(n1.x, n1.y, n0.x, n0.y, n0.radius);
        return start[0];
      })
      .attr('y1', function (link) {
        var n0 = node_of_name[link[0]];
        var n1 = node_of_name[link[1]];
        var start = chop_line(n1.x, n1.y, n0.x, n0.y, n0.radius);
        return start[1];
      })
      .attr('x2', function (link) {
        var n0 = node_of_name[link[0]];
        var n1 = node_of_name[link[1]];
        var end = chop_line(n0.x, n0.y, n1.x, n1.y, n1.radius + arrow_length);
        return end[0];
      })
      .attr('y2', function (link) {
        var n0 = node_of_name[link[0]];
        var n1 = node_of_name[link[1]];
        var end = chop_line(n0.x, n0.y, n1.x, n1.y, n1.radius + arrow_length);
        return end[1];
      });
}

function display_graph(graph, set_node)
{
  console.log('displaying graph of '+ graph.nodes.length +' nodes...');
  if (svg == null) init();

  node_of_name = {};

  foreach(graph.nodes, function (node) {
    // Add an index from node name to node:
    node_of_name[node.name] = node;

    // Specify the node radius in the node itself
    node.radius = 30;
  });

  // Find the parent node(s):
  top_nodes = fold(graph.nodes, {}, function (node, t) {
    if (node.parents.length == 0) {
      t[node.name] = true;
    }
    return t;
  });
  if (top_nodes.length == 0) { alert('No node with no parent!?'); }

  d3.select('svg#graph').on('click', function(){  select_node(null, set_node); });

  nodes.length = 0;
  foreach(graph.nodes, function(n) { nodes.push(n); });
  node = node.data(nodes, function (d) { return d.name; });
  node.exit().remove();

  // Append circle first and then text so text is visible
  var new_nodes = node.enter()
      .append('g')
        .attr('class', node_class)
        .attr('id', function (d) { return 'node_' + d.name; });
  new_nodes.append('circle')
        .attr('r', function(d) { return d.radius; })
        .on('click', function(d) {
          d3.event.stopPropagation();
          select_node(d, set_node);
        });
  new_nodes.append('text')
        .text(node_label)
        .on('click', function(d, e) {
          d3.event.stopPropagation();
          select_node(d, set_node);
        });
  node = new_nodes.merge(node);

  node.call(d3.drag()
       .on('start', dragstarted)
       .on('drag', dragged)
       .on('end', dragended));

  // Now links

  links.length = 0;
  links = map (graph.links, function (link) {
    return { 'source': link[0], 'target': link[1] };
  });

  link = link.data(graph.links, function (d) { return d ? link_name(d) : 'unknown_' + this.id; });
  link.exit().remove();

  var new_links = link.enter()
      .append('g')
        .classed('link', true)
        .attr('id', function (d) { return 'link_' + link_name(d); });
  new_links.append('line')
        .attr('x1', '0')
        .attr('y1', '0')
        .attr('x2', '0')
        .attr('y2', '0');
  link = new_links.merge(link);

  simulation.nodes(nodes);
  simulation.force('link').links(links);
  simulation.alpha(1).restart();

  function dragstarted(d)
  {
    if (!d3.event.active) simulation.alphaTarget(0.3).restart();
    d.fx = d.x;
    d.fy = d.y;
  }

  function dragged(d)
  {
    d.fx = d3.event.x;
    d.fy = d3.event.y;
  }

  function dragended(d)
  {
    if (!d3.event.active) simulation.alphaTarget(0);
    d.fx = null;
    d.fy = null;
  }
}
