#! /usr/bin/env python3
# coding: utf-8
import argparse
import json
import errno
import ipaddress
import socket
import struct
import sys, os
import time

####################################################################################################
# ROS stuff

import rospy
from message_filters import Subscriber, ApproximateTimeSynchronizer
from std_msgs.msg import Float64MultiArray, MultiArrayDimension
from sensor_msgs.msg import JointState
from collections import namedtuple
from typing import Dict
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

DataMessageInfo = namedtuple('DataMessageInfo', ['topic', 'size'])

class ROSManager:
    control_subscriber_topic = None
    control_publisher_topic = None
    joint_command_size = None
    joint_state_size = None
    publisher = None
    data_publishers = {}
    joint_command_message = None
    data_subscriber_info = {}
    data_publisher_info = {}
    rate = None
    new_msgs = {}

    def __init__(self,
                 control_subscriber_topic,
                 control_publisher_topic,
                 joint_command_size,
                 joint_state_size,
                 data_subscriber_info: Dict[int, DataMessageInfo],
                 data_publisher_info: Dict[int, DataMessageInfo],
                 rate):
        rospy.init_node('vmc_control')
        self.rate = rospy.Rate(rate)

        # Control messages
        self.control_subscriber_topic = control_subscriber_topic
        self.control_publisher_topic = control_publisher_topic
        self.joint_command_size = joint_command_size
        self.joint_state_size = joint_state_size
        self.joint_sub = Subscriber(control_subscriber_topic, JointState)
        rospy.Subscriber(control_subscriber_topic, JointState, self._control_subscriber_callback)
        self.publisher = rospy.Publisher(control_publisher_topic, Float64MultiArray, queue_size=1)
        self.joint_command_message = Float64MultiArray()
        dim = MultiArrayDimension()
        dim.size = joint_command_size
        dim.stride = 1
        dim.label = "joint_effort"
        self.joint_command_message.layout.dim.append(dim)
        self.joint_command_message.data = [0.0] * joint_command_size

        # Data messages
        self.data_subscriber_info = data_subscriber_info
        for k, v in self.data_subscriber_info.items():
            size = v.size
            def f(self, msg):
                assert len(msg.data) == size
                self.new_msgs[k] = msg
            rospy.Subscriber(v.topic, Float64MultiArray, f)
        self.data_publisher_info = data_publisher_info
        for k, v in self.data_publisher_info.items():
            self.data_publishers[k] = rospy.Publisher(v.topic, Float64MultiArray, queue_size=1)

    def _control_subscriber_callback(self, joint_msg):
        assert self.joint_state_size%2 == 0 
        assert len(joint_msg.position) == self.joint_state_size//2
        assert len(joint_msg.velocity) == self.joint_state_size//2
        self.new_msgs[0] = joint_msg

        
class JointCommand:
    def __init__(self, sequence_number, timestamp, torques):
        self.sequence_number = sequence_number
        self.timestamp = timestamp
        self.torques = torques

####################################################################################################
# IPC to communicate with Julia

STATE_WAITING = 0
STATE_WARMUP = 1
STATE_ACTIVE = 2
STATE_STOPPED = 3

class IPCManager:
    # Inputs
    listen_ip = None
    listen_port = None
    joint_command_size = None
    joint_state_size = None
    data_sub_fmts = {}
    data_pub_sizes = {}
    # Constants
    publish_fmt = None
    state_fmt = None
    # State
    command_socket = None
    command_stream = None
    data_socket = None
    send_data_socket = None
    state = None
    sequence_number = None

    def __init__(self,
                 listen_ip,
                 listen_port,
                 joint_command_size,
                 joint_state_size,
                 data_subscriber_info: Dict[int, DataMessageInfo],
                 data_publisher_info: Dict[int, DataMessageInfo]):
        self.listen_ip = listen_ip
        self.listen_port = listen_port
        self.joint_command_size = joint_command_size
        self.joint_state_size = joint_state_size
        # ! indicates network endianness, Q for unsigned 64 bit integer, d for 64 bit float
        # Control messages: (timestamp, message_type, sequence_num, *contents)
        self.state_fmt = '!QQQ' + 'd' * joint_state_size

        # Data messages: (timestamp, message_type, *contents)
        self.data_sub_fmts = {}
        for k, v in data_subscriber_info.items():
            self.data_sub_fmts[k] = '!QQ' + 'd' * v.size

        # Messages to publish are sent from Julia in a single serialised packet, sorted by ID
        self.publish_fmt = '!QQ' + 'd' * joint_command_size
        self.data_pub_sizes = [(0, joint_command_size)]
        for k, v in sorted(data_publisher_info.items(), key=lambda x: x[0]):
            self.publish_fmt += 'd' * v.size
            self.data_pub_sizes.append((k, v.size))

    def __enter__(self):
        #print("Waiting for connection")
        self.command_socket = self._wait_for_connection()
        # Setup data socket
        (bound_ip, bound_port) = self.command_socket.getsockname()
        self.data_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP
        self.data_socket.bind((bound_ip, bound_port))
        self.data_socket.setblocking(False)        
        
        self.send_data_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP
        self.send_data_socket.setblocking(False)
        (remote_addr, remote_port) = self.command_socket.getpeername()
        self.send_data_socket.connect((remote_addr, remote_port))
        self.command_stream = self.command_socket.makefile('rw')

        self.state = STATE_WAITING
        self.sequence_number = 1

    def _wait_for_connection(self):
        address_family = socket.AF_INET if listen_ip.version == 4 else socket.AF_INET6
        tcp_server = socket.socket(address_family, socket.SOCK_STREAM) # TCP
        tcp_server.bind((listen_ip.exploded, listen_port))
        tcp_server.listen()
        tcp_server.settimeout(5.0)
        while not rospy.is_shutdown():
            try:
                time.sleep(0.0)
                command_socket, (remote_addr, remote_port) = tcp_server.accept()
                print(f"Connection from {remote_addr}:{remote_port}")
                break
            except socket.timeout:
                print("Timeout waiting for connection, retrying.")
        if rospy.is_shutdown():
            raise Exception("ROS was shut down.")
        command_socket.setblocking(False) # Readline will return an empty string if no data is available
        tcp_server.close() # Only allow one connection at a time
        return command_socket


    def __exit__(self, exc_type, exc_value, traceback):
        print("Closing connection")
        self.command_stream.write("STOP\n")
        self.command_stream.flush()
        time.sleep(1.0)
        self.command_stream.close()
        self.command_socket.close()
        self.data_socket.close()
        
        self.command_socket = None
        self.command_stream = None
        self.data_socket = None
        self.send_data_socket = None
        self.state = None

    def recv_data_from_julia(self):
        pub_data = {}
        command = None
        try:
            # UDP - only 1 message per recv. Allow capacity for largest message.
            n_bytes = struct.calcsize(self.publish_fmt)
            data = self.data_socket.recv(n_bytes)

            timestamp = data[0]
            sequence_number = data[1]
            torques = data[2:2+self.joint_command_size]
            assert len(torques) == self.joint_command_size
            command = JointCommand(sequence_number, timestamp, torques)

            # Extract other data contents
            pub_data = {}
            curr_index = 2+self.joint_command_size
            for id, size in self.data_pub_sizes:
                pub_data[id] = data[curr_index:curr_index+size]
                curr_index += size
        except socket.error as e:
            if e.errno not in (errno.EAGAIN, errno.EWOULDBLOCK):
                print(f"Failed to recv: {e}")
                raise e
            return None
        return (pub_data, command)

    def send_data_to_julia(self, timestamp, data_type, data):
        # Determine packet format. 0 is for joint states, otherwise format is from data_sub_fmts (1-indexed)
        if data_type == 0:
            message = struct.pack(self.state_fmt, 
                timestamp,
                data_type,
                self.sequence_number,
                *data
            )
            self.sequence_number += 1
        else:
            message = struct.pack(self.data_sub_fmts[data_type], 
                timestamp,
                data_type,
                *data
            )
        # print(sequence_number)
        (remote_addr, remote_port) = self.command_socket.getpeername()
        result = self.send_data_socket.sendto(message, (remote_addr, remote_port))
        # Check if the entire packet was sent
        if result != len(message):
            print(f"Failed to send: Sent {result} bytes, expected {len(message)} bytes.")
            exit(1)


####################################################################################################

def loop_waiting(socket_manager):
    print("State: WAITING")
    while not rospy.is_shutdown():
        command = socket_manager.command_stream.readline()
        #print(command=="")
        if command == "START\n":
            return STATE_WARMUP
        elif command == "":
            # Send state to julia every 100ms
            if len(ros_manager.new_msgs) > 0: # New msg received from ROS subscriber
                forward_data_to_julia(socket_manager, ros_manager, warmup=True)
                if 0 in ros_manager.new_msgs:
                    print("Sent initial state")
            else:
                print("No state message received from ROS yet... retrying")
            time.sleep(0.5)
        else:
            print(f"Unexpected command in state WAITING: \"{command}\".")
            return STATE_STOPPED

def forward_data_to_julia(socket_manager: IPCManager, ros_manager: ROSManager, warmup=False):
    for msg_type, msg in ros_manager.new_msgs.items():
        # During warmup, only control messages are processed
        if warmup and msg_type != 0:
            continue

        msg_vec = []
        if msg_type == 0:
            # Joint states
            msg_vec.extend(msg.position)
            msg_vec.extend(msg.velocity)
        else:
            msg_vec = msg.data
        socket_manager.send_data_to_julia(time.time_ns(), msg_type, msg_vec) # Send to julia
    ros_manager.new_msgs = {}  # Clear new messages

def send_recv_send_recv_wait(socket_manager: IPCManager, ros_manager: ROSManager, set_zero=False):
    # print(ros_manager.new_msg.position)
    # print(ros_manager.new_targets_msg)
    if len(ros_manager.new_msgs) > 0: # New msgs received from ROS subscriber
        forward_data_to_julia(socket_manager, ros_manager)
    data, command = socket_manager.recv_data_from_julia()
    if command is not None: # New torque command received from julia
        if set_zero:
            ros_manager.joint_command_message.data = 0 * command.torques
        else:
            ros_manager.joint_command_message.data = command.torques
        ros_manager.publisher.publish(ros_manager.joint_command_message) # Publish via ROS
    if len(data) > 0:  # New data to publish
        for k, v in data.items():
            msg = Float64MultiArray()
            msg.data = v
            ros_manager.data_publishers[k].publish(msg)
    ros_manager.rate.sleep()

def loop_warmup(socket_manager, ros_manager):
    #print("State: WARMUP")
    while not rospy.is_shutdown():
        command = socket_manager.command_stream.readline()
        #print("inside warmup")
        #print(command)
        if command == "":
            #print(10000)
            # Send zero torques only during warmup
            send_recv_send_recv_wait(socket_manager, ros_manager, set_zero=True)
        elif command == "WARMUP_DONE\n":
            return STATE_ACTIVE
        elif command == "STOP\n":
            return STATE_STOPPED
        else:
            print(f"Unexpected command in state WARMUP: \"{command}\".")
            return STATE_STOPPED

def loop_active(socket_manager, ros_manager):
    print("State: ACTIVE")
    while not rospy.is_shutdown():
        command = socket_manager.command_stream.readline()
        if command == "":
            send_recv_send_recv_wait(socket_manager, ros_manager)
        elif command == "STOP\n":
            return STATE_STOPPED
        else:
            print(f"Unexpected command in state ACTIVE: \"{command}\".")
            return STATE_STOPPED

####################################################################################################
# Main

if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--config_file", type=str, required=True)

    # Parse config file
    config_file = parser.parse_args().config_file
    with open(config_file) as f:
        cfg = json.load(f)

    # Extract relevant parameters
    joint_command_size = cfg["ros"]["control"]["joint_commands"]["size"]
    try:
        joint_state_size = cfg["ros"]["control"]["joint_states"]["size"]
    except:
        joint_state_size = 2 * joint_command_size
    joint_commands_topic = cfg["ros"]["control"]["joint_commands"]["topic"]
    joint_state_topic = cfg["ros"]["control"]["joint_states"]["topic"]

    # Handle multiple topics via different packets (and sizes)
    # Subscribers
    data_subscriber_info = {}
    for entry in cfg["ros"]["data"]["subscriber"]:
        if entry["id"] == 0:
            raise Exception("The subscriber ID 0 is reserved for control messages")
        if entry["id"] < 0:
            raise Exception("Subscriber ID must be non-negative")
        if entry["id"] in data_subscriber_info:
            raise Exception("Subscriber ID must be unique")
        data_subscriber_info[entry["id"]] = \
            DataMessageInfo(entry["topic"],
                            entry["data_point_size"] * entry["data_points"])
        
    # Publishers
    data_publisher_info = {}
    for entry in cfg["ros"]["data"]["publisher"]:
        if entry["id"] == 0:
            raise Exception("The publisher ID 0 is reserved for control messages")
        if entry["id"] < 0:
            raise Exception("Publisher ID must be non-negative")
        if entry["id"] in data_publisher_info:
            raise Exception("Publisher ID must be unique")
        data_publisher_info[entry["id"]] = \
            DataMessageInfo(entry["topic"],
                            entry["data_point_size"] * entry["data_points"])

    # Network config
    listen_port = cfg["listen_port"] if "listen_port" in cfg else 25342
    listen_ip = ipaddress.ip_address(cfg["listen_ip"] if "listen_ip" in cfg else "127.0.0.1")
    auto_restart = cfg["auto_restart"] if "auto_restart" in cfg else True
    rate = cfg["rate"] if "rate" in cfg else 1000

    # Setup ROS
    # rospy.init_node('vmc_control')
    ros_manager = ROSManager(
        joint_state_topic,
        joint_commands_topic,
        joint_command_size,
        joint_state_size,
        data_subscriber_info,
        data_publisher_info,
        rate
    )

    # Communication with Julia
    socket_manager = IPCManager(
        listen_ip,
        listen_port,
        joint_command_size,
        joint_state_size,
        data_subscriber_info,
        data_publisher_info
    )

    while not rospy.is_shutdown():
        with socket_manager:
            state = STATE_WAITING
            while not rospy.is_shutdown():
                try:
                    if state == STATE_WAITING:
                        state = loop_waiting(socket_manager)
                    elif state == STATE_WARMUP:
                        state = loop_warmup(socket_manager, ros_manager)
                    elif state == STATE_ACTIVE:
                        state = loop_active(socket_manager, ros_manager)
                    elif state == STATE_STOPPED:
                        break # Loop back to waiting for a new connection
                    else:
                        raise Exception(f"Invalid state: {state}")
                except Exception as e:
                    print(f"Unhandled Exception: {e}")
                    # time.sleep(0.2)
                if not auto_restart:
                    break
        if not auto_restart:
            break

