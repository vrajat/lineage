import React, {Component} from "react";
import axios from "axios";
import {Button} from "react-bootstrap";
import Form from "react-bootstrap/Form";
import Col from "react-bootstrap/Col";
import styled from "styled-components";

const CenteredCol = styled(Col)`
  display: flex;
  align-items: center;
`;

class NodeInput extends Component {
  constructor(props) {
    super(props);
    this.state = {
      node: '',
      start: '',
      end: '',
      showAlert: false,
      errorMessage: "",
      userError: false,
      graph: {}
    };

    this.handleChange = this.handleChange.bind(this);
    this.handleSubmit = this.handleSubmit.bind(this);
  }

  handleChange(event) {
    const target = event.target;
    const value = target.value;
    const name = target.name;

    this.setState({
      [name]: value
    });
  }

  handleSubmit(event) {
    console.log('Table submitted: ' + this.state.node);
    let self = this;
    const payload = {
      predecessors: true,
      node: this.state.node,
      start: this.state.start,
      end: this.state.end
    };

    const url = "/api/dag";
    axios({
      method: "get",
      url: url,
      params: payload,
      headers: {
        "Content-Type": "application/json",
        "Accept": "application/json"
      }
    }).then(function(response) {
      'errorMessage' in response.data ?
          self.setState({
            showAlert: true, userError: true,
            errorMessage: response.data.errorMessage
          }):
          self.props.setGraph(response.data);
    }).catch(function(error) {
      console.log(error);
      self.setState({
        showAlert: true, userError: false,
        errorMessage: "Internal Error. Please contact Support"
      });
    });

    event.preventDefault();
  }

  render() {
    return(
        <Form onSubmit={this.handleSubmit}>
          <Form.Row>
            <Col>
              <Form.Label>Target Table:</Form.Label>
              <Form.Control type={"text"} name={"node"} value={this.state.node} onChange={this.handleChange}/>
            </Col>
            <Col>
              <Form.Label>Start Date:</Form.Label>
              <Form.Control type={"text"} name={"start"} value={this.state.start} onChange={this.handleChange}/>
            </Col>
            <Col>
              <Form.Label>End Date:</Form.Label>
              <Form.Control type={"text"} name={"end"} value={this.state.end} onChange={this.handleChange}/>
            </Col>
            <CenteredCol>
              <Button variant={"primary"} type={"submit"}>
                Submit
              </Button>
            </CenteredCol>
          </Form.Row>
        </Form>
    );
  }
}

export default NodeInput;
