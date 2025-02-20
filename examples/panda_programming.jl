# Adapted from Omar's script

using DifferentialEquations
using GeometryBasics: Vec3f, Point3f
using GLMakie
using LinearAlgebra
using MeshIO
using StaticArrays
using VMRobotControl
using FileIO, UUIDs
using Printf
using Sockets
using StaticArrays
using .Threads 
using VMRobotControl

using VMRobotControl:
    DEFAULT_GRAVITY,
    DEFAULT_F_SETUP,
    DEFAULT_F_CONTROL,
    robot_ndof

using VMRobotControl: remake

ROSPY_LISTEN_PORT = 25342

const START = "START"
const WARMUP_DONE = "WARMUP_DONE"
const STOP = "STOP"
const STOP_ACK = "STOP_ACK"

mutable struct RobotStatePacket
    timestamp::UInt64
    sequence_number::UInt64
    state::Vector{Float64}
end

mutable struct TargetPosPacket
    target_pos::Vector{Float64}
end

mutable struct ROSPyConnectionStatus
    sequence_number::UInt64
    received_packets::UInt64
    processed_packets::UInt64
    last_received::RobotStatePacket
    target_pos_received::TargetPosPacket
end

function print_connection_status_summary(connection)
    (;status_lock, status) = connection
    Base.@lock status_lock begin
        percent_received = 100 * status.received_packets / status.sequence_number
        percent_processed = 100 * status.processed_packets / status.sequence_number
        missed = status.sequence_number - status.processed_packets
        # @printf "SN: %i Received: %.3f%% Processed %.3f%% Missed: %i           \r" status.sequence_number percent_received percent_processed missed
    end
end

struct ROSPyClientConnection
    # config
    num_torques::Int
    num_states::Int
    num_targets::Int  # Serialised data
    # Sockets
    command_socket::TCPSocket
    data_socket::UDPSocket
    # State
    status::ROSPyConnectionStatus
    # Async tasks/buffers for sending/receiving data on UPD/TCP sockets
    stop::Threads.Atomic{Bool}
    recv_tcp_task::Task
    recv_tcp_lock::ReentrantLock
    tcp_buffer::IOBuffer
    recv_udp_task::Task
    status_lock::ReentrantLock
    torques::Vector{Float64}
    send_buffer::Vector{UInt8}
    function ROSPyClientConnection(
            command_socket::TCPSocket,
            num_torques::Int,
            num_states::Int,
            num_targets::Int
        )
        # Bind the data socket to the same address as the command socket
        @assert isopen(command_socket)
        bound_ip, bound_port = getsockname(command_socket) 
        data_socket = UDPSocket()
        bind(data_socket, bound_ip, bound_port)
        # Each "target" is a 3D position (x,y,z)
        status = ROSPyConnectionStatus(0, 0, 0, RobotStatePacket(0, 0, zeros(num_states)), TargetPosPacket(zeros(5*num_targets)))
        # Start a task to receive commands on the tcp socket
        stop = Threads.Atomic{Bool}(false)
        recv_tcp_lock = ReentrantLock()
        tcp_buffer = PipeBuffer()
        recv_tcp_task = @async begin
            while !eof(command_socket) && !stop[]
                data = readline(command_socket)
                isempty(data) && continue
                lock(recv_tcp_lock) do
                    write(tcp_buffer, data)
                    write(tcp_buffer, '\n')
                end
                yield()
            end
            nothing
        end
        # Start a task to receive data on the data socket
        status_lock = ReentrantLock()
        recv_udp_task = @async begin
            while !stop[]
                data = recv(data_socket)
                parse_rospy_packet!(status_lock, status, data)
                yield()
            end
        end
        # Allocate space for sending data from
        torques = zeros(num_torques)
        send_buffer = Vector{UInt8}(undef, sizeof(Int) + sizeof(Float64) + num_torques * sizeof(Float64))
        new(
            num_torques,
            num_states,
            num_targets, # Not sure what exactly is this
            command_socket,
            data_socket,
            status,
            stop,
            recv_tcp_task,
            recv_tcp_lock,
            tcp_buffer,
            recv_udp_task,
            status_lock,
            torques,
            send_buffer
        )
    end
end

function my_time_ns()
    UInt64(time() * 1e9)
end

function parse_rospy_packet!(status_lock, status::ROSPyConnectionStatus, data::Vector{UInt8})
    Base.@lock status_lock begin
        expected_length = sizeof(UInt64) + sizeof(UInt64) +
         length(status.last_received.state) * sizeof(Float64) + length(status.target_pos_received.target_pos) * sizeof(Float64)
        if length(data) != expected_length
            error("Invalid rospy data packet length: expected $expected_length, got $(length(data))")
        end
        b = IOBuffer(data; read=true, write=false)
        status.last_received.timestamp = ntoh(read(b, UInt64))
        status.last_received.sequence_number = ntoh(read(b, UInt64))
        for i in eachindex(status.last_received.state)
            status.last_received.state[i] = ntoh(read(b, Float64))
        end
        for j in eachindex(status.target_pos_received.target_pos)
            status.target_pos_received.target_pos[j] = ntoh(read(b, Float64))
        end
        status.received_packets += 1
    end
    nothing
end

function _connect(rospy_ip, rospy_port, num_torques::Int, num_states::Int, num_targets::Int)
    command_socket = connect(rospy_ip, rospy_port)
    ROSPyClientConnection(command_socket, num_torques, num_states, num_targets)
end

function _cleanup!(connection::ROSPyClientConnection)
    @info "Closing connection"
    write(connection.command_socket, "$STOP\n")
    # This will gracefully stop the recv tasks unless they are blocked
    connection.stop[] = true 
    sleep(0.1)
    # Closing the data socket will cause the recv_udp_task to throw an error
    close(connection.data_socket) # This causes an error 
    try
        fetch(connection.recv_udp_task)
    catch EOFError
    end
    # Closing the command socket will cause the recv_tcp_task to exit
    close(connection.command_socket)
    fetch(connection.recv_tcp_task)
    connection
end

function with_rospy_connection(f::Function, rospy_ip, rospy_port, num_torques::Int, num_states::Int, num_targets::Int)
    connection = _connect(rospy_ip, rospy_port, num_torques, num_states, num_targets)
    try
        f(connection)
    finally
        _cleanup!(connection)
    end
    connection
end

function check_tcp(connection::ROSPyClientConnection)::Union{Nothing, String}
    if istaskfailed(connection.recv_tcp_task)
        fetch(connection.recv_tcp_task) # Re-throw the error
        error()
    elseif istaskdone(connection.recv_tcp_task)
        error("TCP connection/command socket closed")
    end
    yield()
    line = Base.@lock connection.recv_tcp_lock begin
        readline(connection.tcp_buffer)
    end
    if isempty(line)
        return nothing
    else
        @debug "Received command over TCP: \"$line\""
        return line
    end
end

function _get_new_packet_to_process!(connection::ROSPyClientConnection, packet::RobotStatePacket)
    @assert packet.sequence_number > connection.status.sequence_number
    connection.status.sequence_number = packet.sequence_number
    connection.status.processed_packets += 1
    (deepcopy(connection.status.target_pos_received), deepcopy(connection.status.last_received))
end

function check_udp(connection::ROSPyClientConnection)
    if istaskfailed(connection.recv_udp_task)
        fetch(connection.recv_udp_task) # Re-throw the error
        error()
    elseif istaskdone(connection.recv_udp_task)
        error("UDP data socket closed")
    end
    
    # Then check if we received a packet on the data socket
    Base.@lock connection.status_lock begin
    #print(connection.status.last_received.timestamp)
        if connection.status.last_received.sequence_number > connection.status.sequence_number
            return _get_new_packet_to_process!(connection, connection.status.last_received)
        else
            # @debug "No new UDP message to process"
            return (nothing, nothing)
        end
    end
end

function send_torques(connection::ROSPyClientConnection, sequence_number::UInt64)
    # Send the send buffer on the data socket
    @assert length(connection.send_buffer) == sizeof(Int) + sizeof(Float64) + connection.num_torques * sizeof(Float64)
    b = IOBuffer(connection.send_buffer; read=false, write=true)
    
    write(b, hton(sequence_number))
    write(b, hton(my_time_ns()))
    for torque in connection.torques
        write(b, hton(torque))
    end
    remote_ip, remote_port = getpeername(connection.command_socket)
    data = take!(b)
    @assert length(data) == sizeof(UInt) + sizeof(UInt) + connection.num_torques * sizeof(Float64)
    # @debug "Sending $(length(data)) bytes to $remote_ip:$remote_port"
    send(connection.data_socket, remote_ip, remote_port, data)
end


const STATE_WARMUP = 1
const STATE_ACTIVE = 2
const STATE_STOPPED = 3

function loop_warmup(connection::ROSPyClientConnection, control_func!::Function)
    @info "State: WARMUP"
    start_time = time()
    while true
        yield() # To allow for interrupts
        command = check_tcp(connection)::Union{String, Nothing}
        if isnothing(command)
            data_targets, data = check_udp(connection)
            if !isnothing(data)
                i, t, dt = 0, 0.0, 0.0
                stop = control_func!(connection.torques, data.state, i, data_targets.target_pos, t, dt)
                stop && return STATE_STOPPED # Stop the controller gracefully
                any(isnan, connection.torques) && error("Control function returned NaN torques: $(connection.torques)")
                send_torques(connection, data.sequence_number)
                print_connection_status_summary(connection)
            end
        else
            error("Received unexpected command during warmup: \"$command\"")
        end
        if time() - start_time > 1.0
            write(connection.command_socket, "$WARMUP_DONE\n")
            return STATE_ACTIVE
        end   
    end
end

function loop_active(connection::ROSPyClientConnection, control_func!::Function)
    @info "State: ACTIVE"
    i = 1
    t = 0.0
    while true
        yield() # To allow for interrupts
        command = check_tcp(connection)::Union{String, Nothing}
        if isnothing(command)
            data_targets, data = check_udp(connection)
            if !isnothing(data)
                iswarmup = true
                i, dt, t = let t0 = t; t = time(); (i+1, t - t0, t) end
                stop = control_func!(connection.torques, data.state, i, data_targets.target_pos, t, dt)
                stop && return STATE_STOPPED # Stop the controller gracefully
                any(isnan, connection.torques) && error("Control function returned NaN torques: $(connection.torques)")
                send_torques(connection, data.sequence_number)

                print_connection_status_summary(connection)
            end
        else
            error("Received unexpected command while active: \"$command\"")
        end
    end
end

function warmup_and_activate(connection::ROSPyClientConnection, control_func!::Function; )
    if connection.stop[] != false
        error("Connection already used, create a new connection instead.")
    end

    @info "Sending $START"
    write(connection.command_socket, "$START\n")

    state = STATE_WARMUP
    while true
        if state == STATE_WARMUP
            state = loop_warmup(connection, control_func!)
        elseif state == STATE_ACTIVE
            state = loop_active(connection, control_func!)
        elseif state == STATE_STOPPED
            break
        else
            error("Invalid state: $state")
        end
    end
end

function get_initial_state(connection; retries=100, sleep_time=0.2, max_initial_state_age)
    for i = 1:retries
        Base.@lock connection.status_lock begin
            if connection.status.last_received.sequence_number > 0
                initial_state = deepcopy(connection.status.last_received)
                targets_state = deepcopy(connection.status.target_pos_received)
                t_ns = my_time_ns()           
                if (t_ns - initial_state.timestamp)/1e9 > max_initial_state_age 
                    println("Initial state is too old, $((t_ns - initial_state.timestamp)/1e9)... retrying")
                    # Ensure the state is recent
                else
                    return (targets_state, initial_state)
                end
            end
        end
        i < retries && sleep(sleep_time)
    end
    error("Failed to get initial state after $retries retries")
end

function ros_vm_controller(
        connection::ROSPyClientConnection,
        vms,
        qᵛ; 
        gravity=DEFAULT_GRAVITY,
        f_setup=DEFAULT_F_SETUP,
        f_control=DEFAULT_F_CONTROL,
        E_max=1.0,
        max_initial_state_age=1.0
    )
    # Create cache
    control_cache = new_control_cache(vms, qᵛ, gravity)
    let # Set initial joint state in cache
        target_positions, state = get_initial_state(connection; max_initial_state_age)
        NDOF = robot_ndof(control_cache)
        qʳ = view(state.state, 1:NDOF)
        q̇ʳ = zeros(eltype(control_cache), NDOF)
        target_positions_ = view(target_positions.target_pos, 1:5)
        control_step!(control_cache, 0.0, qʳ, q̇ʳ) # Step at t=0 to set initial state
    end
    
    args = f_setup(control_cache) # Call user setup function

    # Create control callback
    control_func! = let control_cache=control_cache, args=args
        function control_func!(torques, state, i, target_positions, t, dt)
            NDOF = robot_ndof(control_cache)
            @assert length(state) == 2*NDOF
            qʳ = view(state, 1:NDOF)
            q̇ʳ = view(state, NDOF+1:2*NDOF)
            target_positions_ = view(target_positions, 1:5) # 1 target with 3 dimensions each (x,y,z) and 2 mechanism constants
            # Main control step
            f_control(control_cache, target_positions_, t, args, (dt, i)) # Call user control function
            torques .= control_step!(control_cache, t, qʳ, q̇ʳ) # Get torques
            return false
        end
    end
    # Check that stored energy is within bounds
    (E = stored_energy(control_cache)) > E_max && error("Initial stored energy exceeds $(E_max)J, was $(E)J")
    warmup_and_activate(connection, control_func!; )
end

#### Now load the robot and the VMC ###

try
    FileIO.add_format(format"DAE", (), ".dae", [:DigitalAssetExchangeFormatIO => UUID("43182933-f65b-495a-9e05-4d939cea427d")])
catch
end
cfg = URDFParserConfig(;suppress_warnings=true) # This is just to hide warnings about unsupported URDF features
# module_path = joinpath(splitpath(splitdir(pathof(VMRobotControl))[1])[1:end-1])
# module_path = pathof(VMRobotControl)
# robot = parseURDF(joinpath(module_path, "URDFs/franka_description/urdfs/panda_with_gripper.urdf"), cfg)
robot = parseURDF("URDFs/franka_description/urdfs/panda_with_gripper.urdf", cfg)

joint_limits = cfg.joint_limits

add_gravity_compensation!(robot, VMRobotControl.DEFAULT_GRAVITY)

for (i, τ_coulomb) in zip(1:7, [5.0, 5.0, 5.0, 5.0, 3.0, 3.0, 3.0])
    β = 1e-1
    limits = joint_limits["panda_joint$i"]
    isnothing(limits) && continue
    @assert ~isnothing(limits.lower) && ~isnothing(limits.upper)
    add_coordinate!(robot, JointSubspace("panda_joint$i");    id="J$i")
    add_deadzone_springs!(robot, 50.0, (limits.lower+0.1, limits.upper-0.1), "J$i")
    add_component!(robot, TanhDamper(τ_coulomb, β, "J$i");         id="JointDamper$i")
end;

add_coordinate!(robot, FramePoint("panda_hand_tcp", SVector(0., 0., 0)); id="EndEffector")

vms = VirtualMechanismSystem("RobotProgramming", robot)
vm = vms.virtual_mechanism

add_coordinate!(vm, ReferenceCoord(Ref(SVector(0.3, -0.5, 1.0))); id="EndEffectorTarget")

add_coordinate!(vms, CoordDifference(".robot.EndEffector", ".virtual_mechanism.EndEffectorTarget"); id="EE pos error")

add_component!(vms, TanhSpring("EE pos error"; max_force=5.0, stiffness=150.0); id="EE_spring")
add_component!(vms, LinearDamper(10.0, "EE pos error"); id="EE_damper")

function f_setup(cache)
    EndEffector_coord_id = get_compiled_coordID(cache, ".virtual_mechanism.EndEffectorTarget")
    EE_spring_id = get_compiled_componentID(cache, "EE_spring")
    EE_damper_id = get_compiled_componentID(cache, "EE_damper")
    return (EndEffector_coord_id, EE_spring_id, EE_damper_id)
end

function f_control(cache, target_positions, t, setup_ret, extra)
    EndEffector_coord_id, EE_spring_id, EE_damper_id = setup_ret
    cache[EndEffector_coord_id].coord_data.val[] = SVector(target_positions[1], target_positions[2], target_positions[3])
    if cache[EE_spring_id].stiffness != target_positions[4]
        cache[EE_spring_id] = remake(cache[EE_spring_id]; stiffness=target_positions[4])
    end
    if cache[EE_damper_id].damping != target_positions[5]
        cache[EE_damper_id] = remake(cache[EE_damper_id]; damping=target_positions[5])
    end
    nothing 
end

cvms = compile(vms)

qᵛ = Float64[]
with_rospy_connection(Sockets.localhost, ROSPY_LISTEN_PORT, 7, 14, 1) do connection
    ros_vm_controller(connection, cvms, qᵛ; f_control, f_setup, E_max=30.0)
end

# tspan = (0., 20π)
# dcache = new_dynamics_cache(compile(vms))
# q = ([0.0, 0.3, 0.0, -1.8, 0.0, π/2, 0.0], Float64[])
# q̇ = zero_q̇(dcache.vms)
# g = VMRobotControl.DEFAULT_GRAVITY
# prob = get_ode_problem(dcache, g, q, q̇, tspan)
# @info "Simulating robot handover sample."
# sol = solve(prob, Tsit5(); maxiters=1e5, abstol=1e-3, reltol=1e-3); # Low tol to speed up simulation

# # ## Plotting
# # We create a figure with two scenes, for two different camera angles. We plot the robot, the
# # targets, the TCPs, and the obstacles in the scene.
# # We use observables for the time and the kinematics cache, which will be updated in the function
# # `animate_robot_odesolution`, causing any plots that depend upon these observables to be updated.
# fig = Figure(size = (720, 720), figure_padding=0)
# display(fig)
# ls = LScene(fig[1, 1]; show_axis=false)
# cam = cam3d!(ls, camera=:perspective, center=false)
# cam.lookat[] = [0., 0., 0.3]
# cam.eyeposition[] = [1.5, 0., 0.3]
# plotting_t = Observable(0.0)
# plotting_kcache = Observable(new_kinematics_cache(compile(vms)))

# target_scatter_kwargs = (;
#     color=:green, 
#     marker=:+, 
#     markersize=15, 
#     label="Targets",
#     transparency=true ## Avoid ugly white outline artefact on markers
# )
# tcp_scatter_kwargs = (;
#     color=:blue, 
#     marker=:x, 
#     markersize=15, 
#     label="TCPs",
#     transparency=true ## Avoid ugly white outline artefact on markers
# )


# ## Show robot
# robotvisualize!(ls, plotting_kcache;)
# #robotsketch!(ls, plotting_kcache, scale=0.3, linewidth=2.5, transparency=true)

# ## Label target and TCP
# target_1_pos_id = get_compiled_coordID(plotting_kcache[], ".virtual_mechanism.LeftFingerTarget")
# target_2_pos_id = get_compiled_coordID(plotting_kcache[], ".virtual_mechanism.RightFingerTarget")
# target_3_pos_id = get_compiled_coordID(plotting_kcache[], ".virtual_mechanism.HandBaseTarget")

# l_tcp_pos_id = get_compiled_coordID(plotting_kcache[], ".robot.LeftFinger")
# r_tcp_pos_id = get_compiled_coordID(plotting_kcache[], ".robot.RightFinger")
# h_tcp_pos_id = get_compiled_coordID(plotting_kcache[], ".robot.HandBase")

# scatter!(ls, plotting_kcache, [target_1_pos_id, target_2_pos_id, target_3_pos_id]; target_scatter_kwargs...)
# scatter!(ls, plotting_kcache, [l_tcp_pos_id, r_tcp_pos_id, h_tcp_pos_id]; tcp_scatter_kwargs...)

# savepath = joinpath(module_path, "docs/src/assets/random_trial.mp4")
# animate_robot_odesolution(fig, sol, plotting_kcache, savepath; t=plotting_t, fastforward=1.0, fps=20)
