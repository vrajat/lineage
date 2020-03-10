import React, {Component} from "react";
import Cytoscape from "cytoscape"
import CytoscapeComponent from "react-cytoscapejs";
import {Container, Row} from "react-bootstrap";
import getRandomName from "../utils/namesgenerator"
import "tippy.js/themes/light.css"
import _ from 'lodash';

import klay from 'cytoscape-klay';
import tippy from "tippy.js";
import popper from "cytoscape-popper";
import NodeInput from "./nodeInput";

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
  constructor(props) {
    super(props);
    this.state = {
      dag: CytoscapeComponent.normalizeElements({}),
      layout: {
        name: 'klay',
        klay: {
          thoroughness: 30,
          direction: 'RIGHT',
          nodePlacement: 'SIMPLE',
        },
      },
      style: [ // the stylesheet for the graph
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
      ],
    };

    this.handleCy = this.handleCy.bind(this);
    this._handleCyCalled = false;
  }

  setDag = (dag) => {
    this.setState({dag: CytoscapeComponent.normalizeElements(dag)});
  };

  componentDidMount() {
    this.setUpListeners()
  }

  setUpListeners() {
    this._cy.ready(function (event) {
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
    this._cy.on('mouseover', 'node', function(event) {
      event.target.tippy.show()
    });
    this._cy.on('mouseout', 'node', function(event) {
      event.target.tippy.hide()
    })
  }

  handleCy(cy) {
    // If the cy pointer has not been modified, and handleCy has already
    // been called before, than we don't run this function.
    if (cy === this._cy && this._handleCyCalled) {
      return;
    }
    this._cy = cy;
    window.cy = cy;
    this._handleCyCalled = true;

    // ///////////////////////////////////// CONSTANTS /////////////////////////////////////////
    const SELECT_THRESHOLD = 100;

    // ///////////////////////////////////// FUNCTIONS /////////////////////////////////////////
    const refreshLayout = _.debounce(() => {
      /**
       * Refresh Layout if needed
       */
      const {
        layout
      } = this.state;

      cy.layout(layout).run()
    }, SELECT_THRESHOLD);

    cy.on('add remove', () => {
      refreshLayout();
    });
  }


  render() {
    return(
        <Container>
          <Row>
            <NodeInput setGraph={this.setDag}/>
          </Row>
          <Row>
            <CytoscapeComponent elements={this.state.dag} style={ { width: '1500px', height: '1000px' } }
                                layout={this.state.layout}
                                stylesheet={this.state.style}
                                cy={this.handleCy}
            />;
          </Row>
        </Container>
    )
  }
}

export default Dag;
