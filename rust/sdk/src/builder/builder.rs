use crate::{
    builder::dag::connect_two_steps,
    traits::{RunnableStep, RunnableStepWithInputReceiver},
    types::transaction_context::TransactionContext,
};
use anyhow::Result;
use petgraph::{
    dot::Config,
    graph::{DiGraph, EdgeReference, NodeIndex},
    prelude::*,
};
use std::{cell::RefCell, collections::HashMap, rc::Rc};
use tokio::task::JoinHandle;

#[derive(Clone, Default, Debug)]
pub struct GraphBuilder {
    // These fields are shared between all the potential instances of the graph
    pub graph: Rc<RefCell<DiGraph<usize, usize>>>,
    pub node_map: Rc<RefCell<HashMap<usize, GraphNode>>>,
    pub node_counter: Rc<RefCell<usize>>,
    // This field is specific to the current instance of the graph
    pub current_node_index: Option<NodeIndex>,
}

impl GraphBuilder {
    pub fn new() -> Self {
        Self {
            graph: Rc::new(RefCell::new(DiGraph::new())),
            node_map: Rc::new(RefCell::new(HashMap::new())),
            node_counter: Rc::new(RefCell::new(0)),
            current_node_index: None,
        }
    }

    pub fn add_step<Input, Output, Step>(
        &mut self,
        step: &RunnableStepWithInputReceiver<Input, Output, Step>,
    ) where
        Input: Send + 'static,
        Output: Send + 'static,
        Step: RunnableStep<Input, Output>,
    {
        let current_node_counter = *self.node_counter.borrow();
        let new_node_index = self.graph.borrow_mut().add_node(current_node_counter);
        self.node_map.borrow_mut().insert(
            current_node_counter,
            GraphNode {
                id: current_node_counter,
                name: step.step.name(),
                step_type: step.type_name(),
                input_type: std::any::type_name::<Input>().to_string(),
                output_type: std::any::type_name::<Output>().to_string(),
                join_handle: None,
                end_step: false,
            },
        );

        *self.node_counter.borrow_mut() += 1;
        self.current_node_index = Some(new_node_index);
    }

    pub fn add_and_connect_step<Input, Output, Step>(
        &mut self,
        step: &RunnableStepWithInputReceiver<Input, Output, Step>,
    ) where
        Input: Send + 'static,
        Output: Send + 'static,
        Step: RunnableStep<Input, Output>,
    {
        let current_node_counter = *self.node_counter.borrow();
        let new_node_index = self.graph.borrow_mut().add_node(current_node_counter);
        self.node_map.borrow_mut().insert(
            current_node_counter,
            GraphNode {
                id: current_node_counter,
                name: step.step.name(),
                step_type: step.type_name(),
                input_type: std::any::type_name::<Input>().to_string(),
                output_type: std::any::type_name::<Output>().to_string(),
                join_handle: None,
                end_step: false,
            },
        );

        self.add_edge_to(new_node_index);
        *self.node_counter.borrow_mut() += 1;
        self.current_node_index = Some(new_node_index);
    }

    pub fn set_end_step(&mut self) {
        let current_node_counter = self.current_node_index.as_ref().unwrap().index();
        self.node_map
            .borrow_mut()
            .get_mut(&current_node_counter)
            .unwrap()
            .end_step = true;
    }

    /*pub fn fanout<Input, Output>(&mut self) {
        let current_node = self.current_node_index.unwrap();
        let fanout_node_index = self.graph.add_node(self.node_counter);
        self.node_counter += 1;

        self.node_map.insert(fanout_node_index.index(), GraphNode {
            id: self.node_counter,
            name: "Fanout".to_string(),
            step_type: "Fanout".to_string(),
            input_type: std::any::type_name::<Input>().to_string(),
            output_type: std::any::type_name::<Output>().to_string(),
            join_handle: None,
        });

        self.add_edge_to(new_node_index);
    }*/

    pub fn set_join_handle(&mut self, node_index: usize, join_handle: JoinHandle<()>) {
        self.node_map
            .borrow_mut()
            .get_mut(&node_index)
            .unwrap()
            .join_handle = Some(join_handle);
    }

    pub fn add_edge_to(&mut self, to: NodeIndex) {
        if let Some(current_node_index) = self.current_node_index {
            self.graph.borrow_mut().add_edge(current_node_index, to, 1);
        }
    }

    pub fn add_edge_from_to(&mut self, from: NodeIndex, to: NodeIndex) {
        self.graph.borrow_mut().add_edge(from, to, 1);
    }

    pub fn dot(&self) -> String {
        let edge_attribute_getter = |_graph, edge_ref: EdgeReference<usize>| {
            let from_node_id = edge_ref.source();
            let node_map = self.node_map.borrow_mut();
            let from_node = node_map.get(&from_node_id.index()).unwrap();

            return format!("label=\"  {}\"", from_node.output_type);
        };

        let last_node_index = self.graph.borrow().node_count() - 1;
        let node_attribute_getter = |_graph, (_node_index, &node_val)| {
            //println!("node_index: {:?}, node_val: {:?}", node_index, node_val);
            //println!("node_map: {:?}", self.node_map);
            let node_map = self.node_map.borrow_mut();
            let node = node_map.get(&node_val).unwrap();

            //let input_output = format!("{} -> {}", &node.input_type, &node.output_type);
            let label = format!("label=\"{}\\n{}\"", &node.name, &node.step_type);
            let shape = if node_val == 0 {
                " shape=invhouse".to_string()
            } else if node.end_step {
                " shape=house".to_string()
            } else {
                " shape=ellipse".to_string()
            };
            return label + &shape;
        };

        // TODO: figure out how to avoid the clone here
        let graph = self.graph.borrow().clone();
        let dot = petgraph::dot::Dot::with_attr_getters(
            &graph,
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
    pub end_step: bool,
}

pub enum CurrentStepHolder<Input, Output, Step>
where
    Input: Send + 'static,
    Output: Send + 'static,
    Step: RunnableStep<Input, Output>,
{
    RunnableStepWithInputReceiver(RunnableStepWithInputReceiver<Input, Output, Step>),
    DanglingOutputReceiver(kanal::AsyncReceiver<TransactionContext<Output>>),
}

pub struct ProcessorBuilder<Input, Output, Step>
where
    Input: Send + 'static,
    Output: Send + 'static,
    Step: RunnableStep<Input, Output>,
{
    pub current_step: Option<CurrentStepHolder<Input, Output, Step>>,
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
        let (input_sender, input_receiver) = kanal::bounded_async(1);
        Self {
            current_step: Some(CurrentStepHolder::RunnableStepWithInputReceiver(
                step.add_input_receiver(input_receiver),
            )),
            graph: GraphBuilder::new(),
        }
    }

    pub fn new_with_runnable_input_receiver_first_step(
        step: RunnableStepWithInputReceiver<Input, Output, Step>,
    ) -> Self {
        Self {
            current_step: Some(CurrentStepHolder::RunnableStepWithInputReceiver(step)),
            graph: GraphBuilder::new(),
        }
    }

    pub fn new_with_fanin_step_with_receivers(
        fanout_step_receivers_and_graphs: Vec<(
            kanal::AsyncReceiver<TransactionContext<Input>>,
            GraphBuilder,
        )>,
        next_step: Step,
        channel_size: usize,
    ) -> ProcessorBuilder<Input, Output, Step>
    where
        Input: Clone + Send + 'static,
        Step: RunnableStep<Input, Output>,
    {
        // Channel connects the output of fanout steps to the input of the next step
        let (connector_sender, connector_receiver) = kanal::bounded_async(channel_size);

        // Spawn the next step here so that we can connect the edges of the fan in steps to it
        let next_step = next_step.add_input_receiver(connector_receiver);
        let mut graph = fanout_step_receivers_and_graphs.first().unwrap().1.clone();
        graph.add_step(&next_step);
        println!(
            "current node index: {:?}",
            graph.current_node_index.unwrap().index(),
        );
        let (next_output_receiver, join_handle) = next_step.spawn(None, channel_size);
        graph.set_join_handle(graph.current_node_index.unwrap().index(), join_handle);

        // Send the results of the fanned out steps to the channel
        for (fanout_step_receiver, gb) in fanout_step_receivers_and_graphs {
            let sender = connector_sender.clone();
            let receiver = fanout_step_receiver.clone();
            tokio::spawn(async move {
                loop {
                    let result = receiver.recv().await;
                    match result {
                        Ok(input) => {
                            sender.send(input.clone()).await.unwrap();
                        },
                        Err(e) => {
                            panic!("Error receiving from previous step for fanout: {:?}", e);
                        },
                    }
                }
            });
            println!(
                "current node index: {:?}",
                graph.current_node_index.unwrap().index(),
            );

            // Connect the fan in step to next step
            graph.add_edge_from_to(
                NodeIndex::new(gb.current_node_index.unwrap().index()),
                NodeIndex::new(graph.current_node_index.unwrap().index()),
            );
        }

        // Return
        ProcessorBuilder {
            current_step: Some(CurrentStepHolder::DanglingOutputReceiver(
                next_output_receiver,
            )),
            graph,
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
        let current_step = self.current_step.take().unwrap();
        let next_step = match current_step {
            CurrentStepHolder::RunnableStepWithInputReceiver(current_step) => {
                self.graph.add_and_connect_step(&current_step);
                let (join_handle, next_step) =
                    connect_two_steps(current_step, next_step, channel_size);
                self.graph
                    .set_join_handle(self.graph.current_node_index.unwrap().index(), join_handle);
                CurrentStepHolder::RunnableStepWithInputReceiver(next_step)
            },
            CurrentStepHolder::DanglingOutputReceiver(output_receiver) => {
                // TODO: HOOK UP THE GRAPH!!!
                let next_step = next_step.add_input_receiver(output_receiver);
                // self.graph.add_and_connect_step(&next_step);
                CurrentStepHolder::RunnableStepWithInputReceiver(next_step)
            },
        };
        // self.graph.add_edge(self.graph.current_node_index - 1, self.graph.current_node_index);

        ProcessorBuilder {
            current_step: Some(next_step),
            graph: self.graph,
        }
    }

    pub fn fanout_broadcast(mut self, num_outputs: usize) -> FanoutBuilder<Input, Output, Step>
    where
        Output: Clone + Send + 'static,
    {
        let previous_output_receiver = match self
            .current_step
            .take()
            .expect("Can not fan out without a prior step")
        {
            CurrentStepHolder::RunnableStepWithInputReceiver(current_step) => {
                self.graph.add_and_connect_step(&current_step);
                let (output_receiver, join_handle) = current_step.spawn(None, num_outputs);
                // TODO: add to graph?
                self.graph
                    .set_join_handle(self.graph.current_node_index.unwrap().index(), join_handle);
                output_receiver
            },
            CurrentStepHolder::DanglingOutputReceiver(output_receiver) => output_receiver,
        };

        let mut output_senders = Vec::new();
        let mut output_receivers = Vec::new();
        for _ in 0..num_outputs {
            let (output_sender, output_receiver) = kanal::bounded_async(0);
            output_senders.push(output_sender);
            output_receivers.push(output_receiver);
        }

        tokio::spawn(async move {
            loop {
                let result = previous_output_receiver.recv().await;
                match result {
                    Ok(input) => {
                        for output_sender in &output_senders {
                            output_sender.send(input.clone()).await.unwrap();
                        }
                    },
                    Err(e) => {
                        panic!("Error receiving from previous step for fanout: {:?}", e);
                    },
                }
            }
        });

        let mut builders = Vec::new();
        for output_receiver in output_receivers {
            builders.push(ProcessorBuilder {
                current_step: Some(CurrentStepHolder::DanglingOutputReceiver(output_receiver)),
                graph: self.graph.clone(),
            });
        }

        FanoutBuilder {
            processor_builders: builders,
            graph: self.graph,
        }
    }

    // You should use connect_to(...).end_and_return_output_receiver(...) instead
    // pub fn end_with_and_return_output_receiver<NextOutput, NextStep>(
    //     mut self,
    //     next_step: NextStep,
    //     channel_size: usize,
    // ) -> (
    //     ProcessorBuilder<Output, NextOutput, NextStep>,
    //     kanal::AsyncReceiver<Vec<NextOutput>>,
    // )
    // where
    //     NextOutput: Send + 'static,
    //     NextStep: RunnableStep<Output, NextOutput>,
    // {
    //     match self.current_step.take() {
    //         None => panic!("Can not end the builder without a starting step"),
    //         Some(current_step) => {
    //             match current_step {
    //                 CurrentStepHolder::RunnableStepWithInputReceiver(current_step) => {
    //                     // Hack for connect_to piping. This is a bit ugly.
    //                     self.current_step = Some(CurrentStepHolder::RunnableStepWithInputReceiver(
    //                         current_step,
    //                     ));
    //                     let mut pb = self.connect_to(next_step, channel_size);

    //                     if let CurrentStepHolder::RunnableStepWithInputReceiver(final_step) =
    //                         pb.current_step.take().unwrap()
    //                     {
    //                         println!(
    //                             "end current node index: {:?}",
    //                             pb.graph.current_node_index.unwrap().index()
    //                         );
    //                         pb.graph.add_and_connect_step(&final_step);
    //                         let (output_receiver, join_handle) =
    //                             final_step.spawn(None, channel_size);
    //                         pb.graph.set_join_handle(
    //                             pb.graph.current_node_index.unwrap().index(),
    //                             join_handle,
    //                         );
    //                         pb.graph.set_end_step();
    //                         println!(
    //                             "end final node index: {:?}",
    //                             pb.graph.current_node_index.unwrap().index()
    //                         );
    //                         (pb, output_receiver)
    //                     } else {
    //                         unreachable!("Dangling output receiver");
    //                     }
    //                 },
    //                 CurrentStepHolder::DanglingOutputReceiver(output_receiver) => {
    //                     let final_step = next_step.add_input_receiver(output_receiver);
    //                     println!(
    //                         "dangling end current node index: {:?}",
    //                         self.graph.current_node_index.unwrap().index()
    //                     );
    //                     self.graph.add_and_connect_step(&final_step);
    //                     let (output_receiver, join_handle) = final_step.spawn(None, channel_size);
    //                     self.graph.set_join_handle(
    //                         self.graph.current_node_index.unwrap().index(),
    //                         join_handle,
    //                     );
    //                     println!(
    //                         "end final node index: {:?}",
    //                         self.graph.current_node_index.unwrap().index()
    //                     );

    //                     let mut pb = ProcessorBuilder {
    //                         current_step: None,
    //                         graph: self.graph,
    //                     };
    //                     pb.graph.set_end_step();
    //                     (pb, output_receiver)
    //                 },
    //             }
    //         },
    //     }
    // }]

    pub fn end_and_return_output_receiver(
        mut self,
        channel_size: usize,
    ) -> (
        ProcessorBuilder<Input, Output, Step>,
        kanal::AsyncReceiver<TransactionContext<Output>>,
    ) {
        match self.current_step.take() {
            None => panic!("Can not end the builder without a starting step"),
            Some(current_step) => match current_step {
                CurrentStepHolder::RunnableStepWithInputReceiver(current_step) => {
                    self.graph.add_and_connect_step(&current_step);
                    let (output_receiver, join_handle) = current_step.spawn(None, channel_size);
                    self.graph.set_join_handle(
                        self.graph.current_node_index.unwrap().index(),
                        join_handle,
                    );
                    self.graph.set_end_step();

                    (
                        ProcessorBuilder {
                            current_step: None,
                            graph: self.graph,
                        },
                        output_receiver,
                    )
                },
                CurrentStepHolder::DanglingOutputReceiver(output_receiver) => {
                    let mut pb = ProcessorBuilder {
                        current_step: None,
                        graph: self.graph,
                    };
                    pb.graph.set_end_step();
                    (pb, output_receiver)
                },
            },
        }
    }
}

pub struct FanoutBuilder<Input, Output, Step>
where
    Input: Send + 'static,
    Output: Send + 'static,
    Step: RunnableStep<Input, Output>,
{
    pub processor_builders: Vec<ProcessorBuilder<Input, Output, Step>>,
    pub graph: GraphBuilder,
}

impl<Input, Output, Step> FanoutBuilder<Input, Output, Step>
where
    Input: Send + 'static,
    Output: Send + 'static,
    Step: RunnableStep<Input, Output>,
{
    pub fn get_processor_builder(&mut self) -> Result<ProcessorBuilder<Input, Output, Step>> {
        if let Some(pb) = self.processor_builders.pop() {
            Ok(pb)
        } else {
            Err(anyhow::anyhow!("No more fanout steps to pop"))
        }
    }
}
