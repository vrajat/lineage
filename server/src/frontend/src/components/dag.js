import React, {Component} from "react";
import Cytoscape from "cytoscape"
import CytoscapeComponent from "react-cytoscapejs";
import {Container, Row} from "react-bootstrap";
import test_graph from "./test_data";
import getRandomName from "../utils/namesgenerator"
import "tippy.js/themes/light.css"

import klay from 'cytoscape-klay';
import tippy from "tippy.js";
import popper from "cytoscape-popper";

Cytoscape.use( popper ).use( klay );

function anonymize(graph) {
  let anonymized_names = {};

  graph.nodes.forEach(node => {
    anonymized_names[node['data']['id']] = getRandomName();
  });
  graph.nodes.forEach(node => {
    let anonymized_node = node;
    anonymized_node['data']['id'] = anonymized_names[node['data']['id']];
  });

  graph.edges.forEach(edge => {
    let anonymized_edge = edge;
    anonymized_edge['data']['source'] = anonymized_names[edge['data']['source']];
    anonymized_edge['data']['target'] = anonymized_names[edge['data']['target']];
  });

  return graph;
}

class Dag extends Component {
  componentDidMount() {
    this.setUpListeners()
  }

  setUpListeners() {
    this.cy.ready(function (event) {
      event.cy.nodes().forEach(function (element) {
        let ref = element.popperRef(); // used only for positioning

        let dummyDom = document.createElement('div');

        element.tippy = tippy(dummyDom, { // tippy options:
          lazy: false,
          onCreate: instance => { instance.popperInstance.reference = ref; },
          content: () => {
            let content = document.createElement('div');

            content.innerHTML = element.id();

            return content;
          },
          theme: 'light',
          trigger: 'manual' // probably want manual mode
        });
      });
    });
    this.cy.on('mouseover', 'node', function(event) {
      event.target.tippy.show()
    });
    this.cy.on('mouseout', 'node', function(event) {
      event.target.tippy.hide()
    })
  }

  render() {
    const dag = CytoscapeComponent.normalizeElements(anonymize(test_graph));
    const layout = { name: 'klay', klay: {
      thoroughness: 30,
        direction: 'RIGHT',
        nodePlacement: 'SIMPLE',
      }, };
    const style = [ // the stylesheet for the graph
      {
        selector: 'node',
        style: {
          'width': 15,
          'height': 15,
          'background-color': '#666',
        }
      },

      {
        selector: 'edge',
        style: {
          'curve-style': 'bezier',
          'width': 1,
          'line-color': '#000',
          'target-arrow-color': '#000',
          'target-arrow-shape': 'vee',
          'arrow-scale': 1
        }
      }
    ];
    return(
        <Container>
          <Row>
            <CytoscapeComponent elements={dag} style={ { width: '1500px', height: '1000px' } }
                                layout={layout}
                                stylesheet={style}
                                cy={(cy) => {this.cy = cy}}
            />;
          </Row>
        </Container>
    )
  }
}

export default Dag;
