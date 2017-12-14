/* vim: ft=css
*/
* { margin: 0; padding: 0; box-sizing: border-box; }
button { -webkit-appearance: none; }
button:focus { outline: 0; }
.actionable:hover, button:hover { background-color: #e4e4e4; }
.actionable { cursor: pointer; border: outset 1.5px #eee; background-color: #ddd; }
.selected { border: outset 1.5px #eee; background-color: #eee; }
.selected-actionable { border: outset 1.5px #eee; background-color: #eee; cursor: pointer; }
@keyframes rotation { to {transform: rotate(360deg);} }
.spinning {
  border: 0;
  background-color: transparent;
  cursor: default;
  animation: rotation 0.3s linear infinite;
  font-weight: 2000;
}
.spinning:hover { background-color: transparent; }
button.icon { width: 3em; height: 3em; }

table { border-collapse: collapse; border-spacing: 10; }
td, th { padding: 0 1em 0 1em; }
tfoot td, th { font-weight: 700; }
span.null { font-style: italic; color: #888; font-size: 80%; }
.spacer { flex-grow: 10; }
.wide { width: 100%; overflow-x: auto; }
.searchbox { display: inline-block; white-space: nowrap; }

#application { margin:0; height: 100%; }
html { background-color: #ddd; }

#global {
  position: fixed;
  top: 0px;
  width: 100%;
  height: 3em;
  border-bottom: 1px solid #888;
  display: flex;
  flex-direction: row;
  justify-content: space-between;
  align-items: center;
  flex-wrap: nowrap;
  overflow: auto;
  background-color: #ddd; }
#global .breadcrumbs
  { display: flex; flex-direction: row; justify-content: flex-start;
    align-items: baseline; font-size: 125%; padding-left: 0.5em; }
#global .breadcrumbs p { padding-left: 0.5em; }
#global .breadcrumbs p.actionable { border: none; color: #118; }
#global .title { display: flex; flex-direction: column; padding-left: 1em; }
#global button { width: 3em; height: 3em; text-align: center; }
#messages {
  position: fixed ; top: 3em; left: 0;
  display: block; width: 100%;
  border-bottom: 1px solid #888;
  font-weight: 700; font-size: 100%;
  color: #000; }
#messages p { padding: 1em 0 1em 1em; }
#messages p.error { background: rgba(240, 30, 20, 0.9); }
#messages span.err-times { font-weight: 1000; margin-left: 1em; }
#messages p.ok { background: rgba(20, 210, 30, 0.9); }

#top { width: 100%; padding-top: 3em; }
h1 { font-size: 110%; padding: 1.5em 0 0.5em 1em; }

#layers div.layer, #layers button.new-layer { display: block; float: left; margin: 5px; }
/* To accomodate the overwritten confirmation dialog: */
#layers div.layer { min-width: 12em; }
#layers div.warning { background-color: #eaa; border: 1.5px solid #eaa; }
#layers div.warning * { visibility: hidden; }
#layers div.overwrite1 { position: relative; top: 0; left:0; width: 0; height: 0; overflow: visible; color: #000; }
#layers div.overwrite2 { position: absolute; top: 0.75em; left:1em; width: 0; height: 0; overflow: visible; }
#layers div.overwrite2 * { visibility: visible; }
#layers div.overwrite2 p { white-space: nowrap; }
#layers p.yes-or-no { margin: auto; padding-top: 1em; font-weight: 700; }
#layers .yes { color: #700; cursor: pointer; padding-left: 3em; padding-right: 1em; }
#layers .no { color: #080; cursor: pointer; padding-left: 1em; }
#layers div.layer div.title { display: flex; flex-direction: row; justify-content: space-between; }
#layers p.name { font-size: 105%; flex-grow: 10; }
#layers .name, #layers .info p { padding-left: 6px; padding-right: 4px; }
#layers .name { padding-top: 4px; }
#layers .info:last-of-type { padding-bottom: 4px; }
#layers button.new-layer { float: none; padding: 0.5em; font-weight: 700; clear: left; }

#nodes thead th { border: 1px solid #ccc; }
#nodes tbody td { white-space: nowrap; padding: 0.3em 1em 0.3em 1em; border-bottom: 1px solid #ccc; }
#nodes tbody td hr { border: 1px solid #aaa; }
#nodes td.export { text-align: center; }
#nodes thead th p { white-space: nowrap; }
#nodes thead th label.searchbox { margin-left: 1em; }

#editor { padding-left: 1em; }
#editor h2 { font-size: 100%; margin: 1em 0 0.25em 0; }
#editor textarea { resize: none; }
#editor div.input { margin-bottom: 0.3em; }
#editor input[type="text"] { margin-left: 1em; }
#editor button { padding: 0.5em 1em 0.5em 1em; font-weight:700; font-size:100%; text-align: center; margin-right: 1em; margin-bottom: 2em; }

#inputs li {
  float: left;
  list-style-type: decimal; list-style-position: outside;
  margin: 0.5em 0em 0.3em 1em; }
#inputs + div { clear: left; }
#inputs li .label { font-size: 100%; }

#output table { margin-top: 1em; }

#operation div.operation { margin-left: 1em; }
#operation pre {
  display: inline-block;
  text-align: left;
  font-size: 120%;
  margin-bottom: 1em; }

#timechart {
  padding-bottom: 10em; /* to help discover the SVG */
  padding-top: 1em;
  margin-left: auto;
  margin-right: auto; }
#timechart>p {
  padding-left: 2em;
  font-weight: 700; font-size: 100%; }

#timechart .chart-buttons {
  display: inline-block;
  padding: 0.5em 0.3em 1em 1em; }
#timechart button { padding: 5px; }
#timechart svg.chart { display: block; }

#layers p.error { background: rgb(240, 30, 20); }
span.label { margin-right: 0.2em; color: #222; font-size: 80%; font-weight: 700; }
span.value { margin-right: 1em; color: #003; font-size: 85%; }

div.tabs {
  width: 100%;
  display: flex;
  flex-direction: row;
  flex-wrap: nowrap;
  justify-content: flex-start;
  align-items: baseline;
}

div.tab { padding: 0.5em 1em 0.5em 1em; font-weight: 700; font-size: 100%; }
th p { margin: 0px; }
th p.type { font-size: 60%; font-style: italic; }
td.number, td.float, td.u8, td.u16, td.u32, td.u64, td.u128, td.i8,
td.i16, td.i32, td.i64, td.i128 { text-align: right; font-family: monospace; }

p.nodata { font-weight: 600; font-style: italic; margin: 1em; }

div.tab div { float: left; min-height: 1em; height: 1em; display: flex; padding: 0 1em 0 1em; }
div.tab div { background-color: #fff; color: #aaa; }
div.tab div.selected { background-color: #ccc; color: #000; }

/* Specific for alerter: */

li { margin-left: 1em; }
.contact.changed { background-color: orange; }

ul.team-members {
  display: flex;
  flex-direction: row;
  flex-wrap: wrap;
  justify-content: flex-start;;
  align-items: stretch;
}

ul.team-members > li {
  display: block;
  border: 5px solid #d4d4d4;
  margin: 5px;
  padding: 1em;
  position: relative;
	min-width: 200px;
}

p.oncaller-name {
	display: inline-block;
	font-size: 140%;
}
p.inhibition { display: inline-block; }

button.tile-save { position: absolute; right: 1em; bottom: 1em; }

div.team-list h2 { margin-top: 1.5em; }
div.team-list div.team-info:first-child h2 { margin-top: .5em; }

div.team-empty { font-style: italic; }

div.inhibitions-outer {
  background-image: url("data:image/svg+xml;base64,BASE64([
<svg width="800" height="400" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <pattern id="Stripes" patternUnits="userSpaceOnUse"
             x="0" y="0" width="40" height="40" >
      <rect fill="#d0e070" width="40" height="40"/>
      <line x1="-40" y1="40" x2="40" y2="-40" stroke="#80c0a5" stroke-width="13"/>
      <line x1="0" y1="40" x2="40" y2="0" stroke="#80c0a5" stroke-width="13"/>
      <line x1="0" y1="80" x2="80" y2="0" stroke="#80c0a5" stroke-width="13"/>
    </pattern> 
  </defs>

  <rect fill="url(#Stripes)" width="800" height="400" />
</svg>])");
	background-repeat: repeat;
  padding: 1em;
  border: 2px solid #80c0a5;
}
div.inhibitions {
  background-color: #eee;
  padding: 1em;
  border: 2px solid #80c0a5;
}

div.add-members {
	display: flex;
	flex-direction: row;
	flex-wrap: nowrap;
	justify-content: flex-start;
	align-items: center;
}
div.add-members-from {
	display: flex;
	flex-direction: column;
  flex-wrap: nowrap;
	align-items: center;
	margin-right: 1em;
}
div.add-members button {
	margin-left: 1em;
	vertical-align: middle;
}

h2 span.team-name { font-style: italic; }
h2 span.title-smaller { margin-left: 1em; font-size: 70%; }

p.explanations, p.oncaller-other-teams { font-style: italic; font-size: 90%; }

span.sqlite-placeholder { margin-right: 1em; }
span.sqlite-placeholder:after { content: ":"; }
p.sqlite-placeholder-help { display: inline-block; }
