import React, {Component} from "react";
import Cytoscape from "cytoscape"
import CytoscapeComponent from "react-cytoscapejs";
import {Container, Row} from "react-bootstrap";
import graph from "./test_data";

import klay from 'cytoscape-klay';
import tippy from "tippy.js";
import popper from "cytoscape-popper";

Cytoscape.use( popper ).use( klay );

class Dag extends Component {
  componentDidMount() {
    this.setUpListeners()
  }

  setUpListeners() {
    console.log(this.cy);
    this.cy.ready(function (event) {
      event.cy.nodes().forEach(function (element) {
        console.log(element);
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
    const dag = CytoscapeComponent.normalizeElements(graph);
    const layout = { name: 'klay', klay: {
      thoroughness: 30,
        direction: 'RIGHT',
        nodePlacement: 'SIMPLE',
      }, };
    const style = [ // the stylesheet for the graph
      {
        selector: 'node',
        style: {
          'background-color': '#666',
        }
      },

      {
        selector: 'edge',
        style: {
          'width': 3,
          'line-color': '#111',
          'target-arrow-color': '#ccc',
          'target-arrow-shape': 'triangle'
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
