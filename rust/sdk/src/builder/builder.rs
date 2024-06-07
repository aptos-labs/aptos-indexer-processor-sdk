use std::collections::HashMap;
use petgraph::dot::Config;
use tokio::task::JoinHandle;
use crate::builder::dag::connect_two_steps;
use crate::traits::{RunnableStep, RunnableStepWithInputReceiver};
use petgraph::graph::{DiGraph, EdgeReference, NodeIndex};
use petgraph::prelude::*;

#[derive(Default, Debug)]
pub struct GraphBuilder {
    pub graph: DiGraph<usize, usize>,
    pub node_map: HashMap<usize, GraphNode>,
    pub node_counter: usize,
    pub current_node_index: Option<NodeIndex>,
}

impl GraphBuilder {
    pub fn new() -> Self {
        Self {
            graph: DiGraph::new(),
            node_map: HashMap::new(),
            node_counter: 0,
            current_node_index: None,
        }
    }

    pub fn add_step<Input, Output, Step>(
        &mut self,
        step: &RunnableStepWithInputReceiver<Input, Output, Step>,
        join_handle: Option<JoinHandle<()>>,
    )
        where
            Input: Send + 'static,
            Output: Send + 'static,
            Step: RunnableStep<Input, Output>
    {
        let new_node_index = self.graph.add_node(self.node_counter);
        self.node_map.insert(self.node_counter, GraphNode {
            id: self.node_counter,
            name: step.step.name(),
            step_type: std::any::type_name::<Step>().to_string(),
            input_type: std::any::type_name::<Input>().to_string(),
            output_type: std::any::type_name::<Output>().to_string(),
            join_handle,
        });

        self.add_edge_to(new_node_index);
        self.node_counter += 1;
    }

    pub fn set_join_handle(&mut self, node_index: usize, join_handle: JoinHandle<()>) {
        self.node_map.get_mut(&node_index).unwrap().join_handle = Some(join_handle);
    }

    pub fn add_edge_to(&mut self, to: NodeIndex) {
        if let Some(current_node_index) = self.current_node_index {
            self.graph.add_edge(current_node_index, to, 1);
        }
        self.current_node_index = Some(to);
    }

    pub fn dot(&self) -> String {
        let edge_attribute_getter = |graph, edge_ref: EdgeReference<usize>| {
            let from_node_id = edge_ref.source();
            let from_node = self.node_map.get(&from_node_id.index()).unwrap();

            return format!("label=\"  {}\"", from_node.output_type);
        };

        let last_node_index = self.graph.node_count() - 1;
        let node_attribute_getter = |graph, (node_index, &node_val)| {
            //println!("node_index: {:?}, node_val: {:?}", node_index, node_val);
            //println!("node_map: {:?}", self.node_map);
            let node = self.node_map.get(&node_val).unwrap();

            //let input_output = format!("{} -> {}", &node.input_type, &node.output_type);
            let label = format!("label=\"{}\\n{}\"", &node.name, &node.step_type);
            let shape = if node_val == 0 {
                " shape=invhouse".to_string()
            } else if node_val == last_node_index {
                " shape=house".to_string()
            } else {
                " shape=ellipse".to_string()
            };
            return label + &shape;
        };

        let dot = petgraph::dot::Dot::with_attr_getters(
            &self.graph,
            // We override the labels anyway
            &[Config::EdgeNoLabel, Config::NodeNoLabel],
            &edge_attribute_getter,
            &node_attribute_getter,
        );
        format!("{}", dot)
    }
}

#[derive(Debug)]
pub struct GraphNode {
    pub id: usize,
    pub name: String,
    pub step_type: String,
    pub input_type: String,
    pub output_type: String,
    pub join_handle: Option<JoinHandle<()>>,
}

pub struct ProcessorBuilder<Input, Output, Step>
    where
        Input: Send + 'static,
        Output: Send + 'static,
        Step: RunnableStep<Input, Output>,
{
    pub current_step: Option<RunnableStepWithInputReceiver<Input, Output, Step>>,
    pub graph: GraphBuilder,
}

impl<Input, Output, Step> ProcessorBuilder<Input, Output, Step>
    where
        Input: Send + 'static,
        Output: Send + 'static,
        Step: RunnableStep<Input, Output>,
{
    pub fn new_with_inputless_first_step(step: Step) -> Self {
        // Assumes that the first step does not actually accept any input
        let (_, input_receiver) = kanal::bounded_async(0);
        Self {
            current_step: Some(step.add_input_receiver(input_receiver)),
            graph: GraphBuilder::new(),
        }
    }

    pub fn new_with_runnable_input_receiver_first_step(step: RunnableStepWithInputReceiver<Input, Output, Step>) -> Self {
        Self {
            current_step: Some(step),
            graph: GraphBuilder::new(),
        }
    }

    pub fn connect_to<NextOutput, NextStep>(
        mut self,
        next_step: NextStep,
        channel_size: usize,
    ) -> ProcessorBuilder<Output, NextOutput, NextStep>
        where
            NextOutput: Send + 'static,
            NextStep: RunnableStep<Output, NextOutput>,
    {
        let (join_handle, next_step) = connect_two_steps(self.current_step.take().unwrap(), next_step, channel_size);
        self.graph.add_step(&next_step, Some(join_handle));
        // self.graph.add_edge(self.graph.current_node_index - 1, self.graph.current_node_index);

        ProcessorBuilder {
            current_step: Some(next_step),
            graph: self.graph,
        }
    }

    pub fn end_with_and_return_output_receiver<NextOutput, NextStep>(self,
                                                                     next_step: NextStep,
                                                                     channel_size: usize,
    ) -> (ProcessorBuilder<Output, NextOutput, NextStep>, kanal::AsyncReceiver<Vec<NextOutput>>)
        where
            NextOutput: Send + 'static,
            NextStep: RunnableStep<Output, NextOutput>,
    {
        let mut pb = self.connect_to(next_step, channel_size);

        let final_step = pb.current_step.take().unwrap();
        pb.graph.add_step(&final_step, None);

        let (output_receiver, join_handle) = final_step.spawn(None, channel_size);
        pb.graph.set_join_handle(pb.graph.current_node_index.unwrap().index() - 1, join_handle);

        (pb, output_receiver)
    }
}