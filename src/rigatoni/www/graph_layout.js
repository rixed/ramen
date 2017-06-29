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

function tweak_graph(graph)
{
  graph.node_of_name = {};

  function fix_type(type) {
    return map(type, function (t) {
      var t = t[1];  // remove rank
      t.typ = keys(t.typ)[0];
      return t;
    });
  }

  foreach(graph.nodes, function (node) {
    // Add an index from node name to node:
    graph.node_of_name[node.name] = node;

    // Simplify node type to help exchanging
    // nodes with PUX (which requires array of same types)
    node.output_type = fix_type(node.output_type);
    node.input_type = fix_type(node.input_type);

    // Specify the node radius in the node itself
    node.radius = 30;
  });
}

// Has to be global because keeping a ref to a previous simulation
// in the event handler of the updated points would cause mayhem.
var simulation;

function display_graph(graph, set_node)
{
  tweak_graph(graph);

  // Find the parent node(s):
  var top_nodes = fold(graph.nodes, {}, function (node, t) {
    if (node.parents.length == 0) {
      t[node.name] = true;
    }
    return t;
  });
  if (top_nodes.length == 0) { alert('No node with no parent!?'); }

  d3.select('svg#graph').on('click', function(){  select_node(null, set_node); });

  var svg = document.getElementById('graph');
  var svgbox = svg.getBoundingClientRect();

  var links = map (graph.links, function (link) {
    return { 'source': link[0], 'target': link[1] };
  });

  update_graph();
  simulation = d3.forceSimulation(graph.nodes)
    .force('link', d3.forceLink(links).id(function(node) { return node.name; }).distance(100))
    .force('charge', d3.forceManyBody().strength(-100))
    .force('center', d3.forceCenter(svgbox.width/2, svgbox.height/2))
    .force('positioning', d3.forceX(svgbox.width/2).strength(function(node) { return top_nodes[node.name] ? 1:0.2; }))
    .force('positioning', d3.forceY(100).strength(function(node) { return top_nodes[node.name] ? 1:0; }))
    .force('collide', d3.forceCollide().radius(function(node) { return node.radius * 1.2; }))
    .alphaDecay(0.03)
    .on('tick', update_graph);

  function update_graph()
  {
    update_links(); // first so that nodes overwrite them.
    update_nodes();
  }

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

  function update_nodes()
  {
    // Bind node data
    var p = d3.select('svg#graph').selectAll('g.node')
      .data(graph.nodes,
            function (d) {
              return d ? d.name : 'unknwown_' + this.id;
            });

    // Delete extra nodes 
    p.exit().remove();

    // Create new elements as required
    function node_label(d) { return d.name; }
    function node_class(d) { return 'node ' + d.type_of_operation.replace(/ /g, '_'); }
    var e =
      p.enter()
        .append('g')
          .attr('class', node_class)
          .attr('id', function (d) { return 'node_' + d.name; });
    // Append circle first and then text so text is visible
    e.append('circle')
      .attr('r', function(node) { return node.radius; })
      .on('click', function(node) {
        d3.event.stopPropagation();
        select_node(node, set_node);
      })
      .call(d3.drag()
         .on('start', dragstarted)
         .on('drag', dragged)
         .on('end', dragended));
    e.append('text')
      .text(node_label)
      .on('click', function(node, e) {
        d3.event.stopPropagation();
        select_node(node, set_node);
      });

    // Update new+old nodes
    var ee = e.merge(p);
    ee.select('text')
        .text(node_label)
        .attr('x', function (node) { return node.x; })
        .attr('y', function (node) { return node.y; });
    ee.select('circle')
        .attr('cx', function (node) { return node.x; })
        .attr('cy', function (node) { return node.y; });
  }

  function update_links()
  {
    function link_name(link) { return link[0] +'_'+ link[1]; }

    // Bind link data
    var p = d3.select('svg#graph').selectAll('g.link')
      .data(graph.links,
            function (d) { return d ? link_name(d) : 'unknown_' + this.id; });

    // Delete extra links
    p.exit().remove();

    // Create new links
    var e =
      p.enter()
        .append('g')
          .classed('link', true)
          .attr('id', function (d) { return 'link_' + link_name(d); });
    e.append('line')
        .attr('x1', '0')
        .attr('y1', '0')
        .attr('x2', '0')
        .attr('y2', '0');

    // Update new+old links
    var ee = e.merge(p);
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
    ee.select('line')
        .attr('x1', function (link) {
          var n0 = graph.node_of_name[link[0]];
          var n1 = graph.node_of_name[link[1]];
          var start = chop_line(n1.x, n1.y, n0.x, n0.y, n0.radius);
          return start[0];
        })
        .attr('y1', function (link) {
          var n0 = graph.node_of_name[link[0]];
          var n1 = graph.node_of_name[link[1]];
          var start = chop_line(n1.x, n1.y, n0.x, n0.y, n0.radius);
          return start[1];
        })
        .attr('x2', function (link) {
          var n0 = graph.node_of_name[link[0]];
          var n1 = graph.node_of_name[link[1]];
          var end = chop_line(n0.x, n0.y, n1.x, n1.y, n1.radius + arrow_length);
          return end[0];
        })
        .attr('y2', function (link) {
          var n0 = graph.node_of_name[link[0]];
          var n1 = graph.node_of_name[link[1]];
          var end = chop_line(n0.x, n0.y, n1.x, n1.y, n1.radius + arrow_length);
          return end[1];
        });
  }
}
