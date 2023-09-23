// https://observablehq.com/@d3/us-state-choropleth@270
import define1 from "./7a9e12f9fb3d8e06@459.js";
import define2 from "./2695158caff5fb0c@563.js";

function _1(md){return(
md`# U.S. State Choropleth

Abortion Sentiment by State, June-August 2022.`
)}

function _key(Legend,chart){return(
Legend(chart.scales.color, {title: "Sentiment Level"})
)}

function _chart(UsStateChoropleth,unemployment,namemap,d3){return(
UsStateChoropleth(unemployment, {
  id: d => namemap.get(d.name),
  value: d => d.rate,
  scale: d3.scaleQuantize,
  domain: [0, 1],
  range: d3.schemeGreens[8],
  title: (f, d) => `${f.properties.name}\n${d?.rate}`
})
)}

function _unemployment(FileAttachment){return(
FileAttachment("unemployment201907.csv").csv({typed: true})
)}

function _5(md){return(
md`This dataset regrettably doesn’t include FIPS numeric identifiers for states; it only has state names. This map lets us look up the FIPS code for a state by name. If your data has FIPS identifiers, you should pass them directly as the _id_ option to the choropleth component; if your data has names, you can import this _namemap_ to look up the corresponding FIPS identifier as needed.`
)}

function _namemap(states){return(
new Map(states.features.map(d => [d.properties.name, d.id]))
)}

function _7(md){return(
md`The UsStateChoropleth component wraps the generic [D3 Choropleth](/@d3/choropleth) component to provide default arguments suitable for rendering a choropleth of U.S. states.`
)}

function _UsStateChoropleth(states,statemesh,Choropleth){return(
function UsStateChoropleth(data, {
  features = states,
  borders = statemesh,
  width = 975,
  height = 610,
  ...options
} = {}) {
  return Choropleth(data, {features, borders, width, height, ...options});
}
)}

function _9(howto){return(
howto("UsStateChoropleth")
)}

export default function define(runtime, observer) {
  const main = runtime.module();
  function toString() { return this.url; }
  const fileAttachments = new Map([
    ["unemployment201907.csv", {url: new URL("./files/DataInput", import.meta.url), mimeType: null, toString}]
  ]);
  main.builtin("FileAttachment", runtime.fileAttachments(name => fileAttachments.get(name)));
  main.variable(observer()).define(["md"], _1);
  main.variable(observer("key")).define("key", ["Legend","chart"], _key);
  main.variable(observer("chart")).define("chart", ["UsStateChoropleth","unemployment","namemap","d3"], _chart);
  main.variable(observer("unemployment")).define("unemployment", ["FileAttachment"], _unemployment);
  main.variable(observer()).define(["md"], _5);
  main.variable(observer("namemap")).define("namemap", ["states"], _namemap);
  main.variable(observer()).define(["md"], _7);
  main.variable(observer("UsStateChoropleth")).define("UsStateChoropleth", ["states","statemesh","Choropleth"], _UsStateChoropleth);
  main.variable(observer()).define(["howto"], _9);
  const child1 = runtime.module(define1);
  main.import("howto", child1);
  const child2 = runtime.module(define2);
  main.import("us", child2);
  main.import("states", child2);
  main.import("statemesh", child2);
  main.import("Choropleth", child2);
  main.import("Legend", child2);
  return main;
}
