# Encode all out msg
# switched user doesnt broadcast

import socket
import threading
import sys
import time
import queue
import os


class Client:
    def __init__(self, username, connection, address):
        self.username = username
        self.connection = connection
        self.address = address
        self.kicked = False
        self.in_queue = True
        self.remaining_time = 100 # remaining time before AFK
        self.muted = False
        self.mute_duration = 0


class Channel:
    def __init__(self, name, port, capacity):
        self.name = name
        self.port = port
        self.capacity = capacity
        self.queue = queue.Queue()
        self.clients = []

def parse_config(config_file: str) -> list:
    """
    Parses lines from a given configuration file and VALIDATE the format of each line. The 
    function validates each part and if valid returns a list of tuples where each tuple contains
    (channel_name, channel_port, channel_capacity). The function also ensures that there are no 
    duplicate channel names or ports. if not valid, exit with status code 1.
    Status: TODO
    Args:
        config_file (str): The path to the configuration file (e.g, config_01.txt).
    Returns:
        list: A list of tuples where each tuple contains:
        (channel_name, channel_port, and channel_capacity)
    Raises:
        SystemExit: If there is an error in the configuration file format.
    """
    # Write your code here...
    with open(config_file, 'r') as channel_file:
        lines = channel_file.readlines()

    channel_data = []
    channel_names = set()
    channel_ports = set()

    if (len(lines) != 1):
        if (len(lines) == 2 or len(lines) < 1):
            sys.exit(1)

    for line in lines:
        data = line.strip().split()
        if len(data) != 4 or data[0] != "channel" or not data[1].isalpha() or not data[2].isdigit() or not data[3].isdigit():
            sys.exit(1)

        channel_name, channel_port, channel_capacity = data[1], int(data[2]), int(data[3])
        if channel_name in channel_names or channel_port in channel_ports or channel_capacity < 1 or channel_capacity > 5 or (channel_port >= 49152 and channel_port <= 65535):
            sys.exit(1)
        
        channel_names.add(channel_name)
        channel_ports.add(channel_port)

        channel_data.append((channel_name, channel_port, channel_capacity))

    return channel_data

    

def get_channels_dictionary(parsed_lines) -> dict:
    """
    Creates a dictionary of Channel objects from parsed lines.
    Status: Given
    Args:
        parsed_lines (list): A list of tuples where each tuple contains:
        (channel_name, channel_port, and channel_capacity)
    Returns:
        dict: A dictionary of Channel objects where the key is the channel name.
    """
    channels = {}

    for channel_name, channel_port, channel_capacity in parsed_lines:
        channels[channel_name] = Channel(channel_name, channel_port, channel_capacity)

    return channels

def quit_client(client, channel) -> None:
    """
    Implement client quitting function
    Status: TODO
    """
    quit_msg = f"[Server message ({time.strftime('%H:%M:%S')})] {client.username} has left the channel."
    # if client is in queue
    if client.in_queue:
        # Write your code here...
        # remove, close connection, and print quit message in the server.
        channel.queue = remove_item(channel.queue, client)
        client.connection.send("EXIT".encode())
        client.connection.shutdown(socket.SHUT_RDWR)
        client.connection.close()
        
        print(quit_msg, flush=True)
        # broadcast queue update message to all the clients in the queue.        
        for index, queued_client in enumerate(list(channel.queue.queue)):
            queue_message = f"[Server message ({time.strftime('%H:%M:%S')})] You are in the waiting queue and there are {len(list(channel.queue.queue)) - index - 1} user(s) ahead of you."
            queued_client.connection.send(queue_message.encode()) 

    # if client is in channel
    else:  
        # Write your code here...
        # remove client from the channel, close connection, and broadcast quit message to all clients.
        channel.clients.remove(client)
        client.connection.send("EXIT".encode())
        client.connection.shutdown(socket.SHUT_RDWR)
        client.connection.close()
        print(quit_msg, flush=True)
        server_broadcast(channel, f"{client.username} has left the channel.")
        # HELP - also to the queued client?

    return
        

def send_client(client, channel, msg) -> None:
    """
    Implement file sending function, if args for /send are valid.
    Else print appropriate message and return.
    Status: TODO
    """
    # Write your code here...
    # if in queue, do nothing
    if client.in_queue:
        return
    else:
        # if muted, send mute message to the client
        if client.muted:
            client.connection.send(f"[Server message ({time.strftime('%H:%M:%S')})] You are still muted for {client.mute_duration} seconds.".encode())

        # if not muted, process the file sending
        else:
            # validate the command structure    
            if (len(msg.split()) != 3):
                client.connection.send(f"[Server message ({time.strftime('%H:%M:%S')})] Usage {msg}".encode())
                return          
            target_client = msg.split()[1]
            file_path = msg.split()[2].strip("\"")
            
           # check for target existance
            for users in channel.clients:
                if users.username == target_client:
                    # check for file existence
                    if not os.path.exists(file_path):
                        client.connection.send(f"[Server message ({time.strftime('%H:%M:%S')})] {file_path} does not exist.".encode())
                        return
                    
                    print(f"[Server message ({time.strftime('%H:%M:%S')})] {client.username} sent {file_path} to {target_client}.", flush=True)
                    
                    # HELP SEND FILE
                    file_name = file_path.split('/')[-1]
                    users.connection.send(f"FILE: {file_name}".encode())
                    file = open(file_name, 'r')
                    users.connection.send(file.read().encode())
                    file.close()

                    client.connection.send(f"[Server message ({time.strftime('%H:%M:%S')})] You sent {file_path} to {target_client}.".encode())
                    return
            
            client.connection.send(f"[Server message ({time.strftime('%H:%M:%S')})] {target_client} is not here.".encode())
            return
    return

def list_clients(client, channels) -> None:
    """
    List all channels and their capacity
    Status: TODO
    """
    # Write your code here...
    for channel in channels.values():
        client.connection.send(f"[Channel] {channel.name} {channel.port} Capacity: {len(channel.clients)}/ {channel.capacity}, Queue: {channel.queue.qsize()}.".encode())
    return

def whisper_client(client, channel, msg) -> None:
    """
    Implement whisper function, if args for /whisper are valid.
    Else print appropriate message and return.
    Status: TODO
    """
    # Write your code here...
    # if in queue, do nothing
    if client.in_queue:
        return
    else:
        # if muted, send mute message to the client
        if client.muted:
            client.connection.send(f"[Server message ({time.strftime('%H:%M:%S')})] You are still muted for {client.mute_duration} seconds.".encode())
            return
        else:
            # validate the command structure
            if (len(msg.split()) <= 2):
                client.connection.send(f"[Server message ({time.strftime('%H:%M:%S')})] Usage {msg}".encode())
                return  
            target_user = msg.split()[1]
            whisper_message = msg.split(' ', 2)[2]
            # validate if the target user is in the channel
            for users in channel.clients:
                if users.username == target_user:
                    users.connection.send(f"[{client.username} whispers to you: ({time.strftime('%H:%M:%S')})] {whisper_message}".encode())
                    print(f"[{client.username} whispers to {target_user}: ({time.strftime('%H:%M:%S')})] {whisper_message}", flush=True)
                    return
            # if target user is in the channel, send the whisper message

            # print whisper server message
            client.connection.send(f"[Server message ({time.strftime('%H:%M:%S')})] {target_user} is not here.".encode())
            return
    return

def switch_channel(client, channel, msg, channels) -> bool:
    """
    Implement channel switching function, if args for /switch are valid.
    Else print appropriate message and return.
    Returns: bool
    Status: TODO
    """
    # Write your code here...
    # validate the command structure
    if not client.muted:
        client.remaining_time = 100
        
    if (len(msg.split()) != 2):
        client.connection.send(f"[Server message ({time.strftime('%H:%M:%S')})] Usage {msg}".encode())
        return False
    switch_channel = msg.split()[1]
    # check if the new channel exists
    if switch_channel not in channels.keys():
        client.connection.send(f"[Server message ({time.strftime('%H:%M:%S')})] {switch_channel} does not exist.".encode())
        return False
    # check if there is a client with the same username in the new channel
    if not check_duplicate_username(client.username, channels[switch_channel], client.connection, switching = True):
        return False
    # if all checks are correct, and client in queue
    if client.in_queue:
        # remove client from current channel queue
        channel.queue = remove_item(channel.queue, client)
        # broadcast queue update message to all clients in the current channel    
        for index, queued_client in enumerate(channel.queue.queue):
            queue_message = f"[Server message ({time.strftime('%H:%M:%S')})] You are in the waiting queue and there are {len(channel.queue) - index - 1} user(s) ahead of you."
            queued_client.connection.send(queue_message.encode())
        # tell client to connect to new channel and close connection
    print(f"[Server message ({time.strftime('%H:%M:%S')})] {client.username} has left the channel.", flush=True)
    server_broadcast(channel, f"{client.username} has left the channel.", muted = client.username)

        #position_client(channels[switch_channel], client.connection, client.username, client)

    # if all checks are correct, and client in channel
        # remove client from current channel
        # tell client to connect to new channel and close connection
        # HELP do i actually do this? client.connection.close()
        #position_client(channels[switch_channel], client.connection, client.username, client)
    client.connection.send(f"SWITCH: {channels[switch_channel].port}".encode())
    quit_client(client, channel)
    return True

def broadcast_in_channel(client, channel, msg) -> None:
    """
    Broadcast a message to all clients in the channel.
    Status: TODO
    """
    # Write your code here...
    # if in queue, do nothing
    if client.kicked:
        sys.stdout.flush()
        return
    if client.in_queue:
        return
    # if muted, send mute message to the client
    if client.muted:
        client.connection.send(f"[Server message ({time.strftime('%H:%M:%S')})] You are still muted for {client.mute_duration} seconds.".encode())
        return
    # broadcast message to all clients in the channel
    for users in channel.clients:
        users.connection.send(f"[{client.username} ({time.strftime('%H:%M:%S')})] {msg}".encode())
    # added    
    print(f"[{client.username} ({time.strftime('%H:%M:%S')})] {msg}", flush=True)
    return
    
def server_broadcast(channel, msg, muted = None) -> None:
    """
    Broadcast a message to all clients in the server.
    Status: Self Made
    """
    for client in channel.clients:
        if client.username != muted:
            client.connection.send(f"[Server message ({time.strftime('%H:%M:%S')})] {msg}".encode())
    return

def client_handler(client, channel, channels) -> None:
    """
    Handles incoming messages from a client in a channel. Supports commands to quit, send, switch, whisper, and list channels. 
    Manages client's mute status and remaining time. Handles client disconnection and exceptions during message processing.
    Status: TODO (check the "# Write your code here..." block in Exception)
    Args:
        client (Client): The client to handle.
        channel (Channel): The channel in which the client is.
        channels (dict): A dictionary of all channels.
    """
    while True:
        if client.kicked:
            break
        try:
            msg = client.connection.recv(1024).decode()
            # check message for client commands
            if msg == "": continue
            elif msg.startswith("/quit"):
                quit_client(client, channel)
                break
            elif msg.startswith("/send"):
                send_client(client, channel, msg)
            elif msg.startswith("/list"):
                list_clients(client, channels)
            elif msg.startswith("/whisper"):
                whisper_client(client, channel, msg)
            elif msg.startswith("/switch"):
                is_valid = switch_channel(client, channel, msg, channels)
                if is_valid:
                    break
                else:
                    continue

            # if not a command, broadcast message to all clients in the channel
            else:
                broadcast_in_channel(client, channel, msg)

            # reset remaining time before AFK
            if not client.muted:
                client.remaining_time = 100
        except EOFError:
            continue
        except OSError:
            break
        except Exception as e:
            print(f"Error in client handler: {e}")
            # remove client from the channel, close connection
            # Write your code here...
            channel.clients.remove(client)
            client.connection.send("EXIT".encode())
            client.connection.shutdown(socket.SHUT_RDWR)
            client.connection.close()
            break

def check_duplicate_username(username, channel, conn, switching = False) -> bool:
    """
    Check if a username is already in a channel or its queue.
    Status: TODO
    """
    # Write your code here...
    clone = True
    for client in channel.clients:
        if client.username == username:
            clone = False
    for client in channel.queue.queue:
        if client.username == username:
            clone = False 
    if not clone:
        conn.send(f"[Server message ({time.strftime('%H:%M:%S')})] {channel.name} already has a user with username {username}.".encode())
        if not switching:
            conn.send("EXIT".encode())
            conn.shutdown(socket.SHUT_RDWR)
            conn.close()
    return clone

def position_client(channel, conn, username, new_client) -> None:
    """
    Place a client in a channel or queue based on the channel's capacity.
    Status: TODO
    """
    # Write your code here...
    if len(channel.clients) < channel.capacity and channel.queue.empty():
        # put client in channel and reset remaining time before AFK
        channel.clients.append(new_client)
        new_client.remaining_time = 100
        new_client.in_queue = False
        server_broadcast(channel, f"{username} has joined the channel.")
        print(f"[Server message ({time.strftime('%H:%M:%S')})] {username} has joined the {channel.name} channel.", flush=True)
    else:
        # put client in queue
        channel.queue.put(new_client)
        new_client.in_queue = True
        new_client.connection = conn
        new_client.connection.send(f"[Server message ({time.strftime('%H:%M:%S')})] Welcome to the {channel.name} waiting room, {username}".encode())
        new_client.connection.send(f"[Server message ({time.strftime('%H:%M:%S')})] You are in the waiting queue and there are {channel.queue.qsize() - 1} user(s) ahead of you.".encode())
    return

def channel_handler(channel, channels) -> None:
    """
    Starts a chat server, manage channels, respective queues, and incoming clients.
    This initiates different threads for chanel queue processing and client handling.
    Status: Given
    Args:
        channel (Channel): The channel for which to start the server.
    Raises:
        EOFError: If there is an error in the client-server communication.
    """
    # Initialize server socket, bind, and listen
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(("localhost", channel.port))
    server_socket.listen(channel.capacity)

    # launch a thread to process client queue
    queue_thread = threading.Thread(target=process_queue, args=(channel,))
    queue_thread.start()

    while True:
        try:
            # accept a client connection
            conn, addr = server_socket.accept()
            username = conn.recv(1024).decode()

            # check duplicate username in channel and channel's queue
            is_valid = check_duplicate_username(username, channel, conn)
            if not is_valid: continue

            welcome_msg = f"[Server message ({time.strftime('%H:%M:%S')})] Welcome to the {channel.name} channel, {username}."
            conn.send(welcome_msg.encode())

            time.sleep(0.1)
            new_client = Client(username, conn, addr)

            # position client in channel or queue
            position_client(channel, conn, username, new_client)

            # Create a client thread for each connected client, whether they are in the channel or queue
            client_thread = threading.Thread(target=client_handler, args=(new_client, channel, channels))
            client_thread.start()
        except EOFError:
            continue

def remove_item(q, item_to_remove) -> queue.Queue:
    """
    Remove item from queue
    Status: Given
    Args:
        q (queue.Queue): The queue to remove the item from.
        item_to_remove (Client): The item to remove from the queue.
    Returns:
        queue.Queue: The queue with the item removed.
    """
    new_q = queue.Queue()
    while not q.empty():
        current_item = q.get()
        if current_item != item_to_remove:
            new_q.put(current_item)

    return new_q

def process_queue(channel) -> None:
    """
    Processes the queue of clients for a channel in an infinite loop. If the channel is not full, 
    it dequeues a client, adds them to the channel, and updates their status. It then sends updates 
    to all clients in the channel and queue. The function handles EOFError exceptions and sleeps for 
    1 second between iterations.
    Status: TODO
    Args:
        channel (Channel): The channel whose queue to process.
    Returns:
        None
    """
    # Write your code here...
    while True:
        try:
            if not channel.queue.empty() and len(channel.clients) < channel.capacity:
                # Dequeue a client from the queue and add them to the channel
                process_client = channel.queue.queue[0]
                channel.queue = remove_item(channel.queue, process_client)
                position_client(channel, process_client.connection, process_client.username, process_client)
                # Send join message to all clients in the channel
                # Update the queue messages for remaining clients in the queue
                # Reset the remaining time to 100 before AFK

                time.sleep(1)
        except EOFError:
            continue

def kick_user(command, channels) -> None:
    """
    Implement /kick function
    Status: TODO
    Args:
        command (str): The command to kick a user from a channel.
        channels (dict): A dictionary of all channels.
    Returns:
        None
    """
    # Write your code here...
    # validate command structure
    if (len(command.split()) != 3):
        return  
    channel_name = command.split()[1]
    username = command.split()[2]
    # check if the channel exists in the dictionary
    for channel in channels.values():
        if channel.name == channel_name:
        # if channel exists, check if the user is in the channel
            # HELP do i also check if the user is in the queue?
            for client in channel.clients:
                if client.username == username:
                # if user is in the channel, kick the user
                    user_found = True
                    client.kicked = True
                    print(f"[Server message ({time.strftime('%H:%M:%S')})] Kicked {username}.", flush=True)
                    quit_client(client, channel)
                    
            # if user is not in the channel, print error message
            if not user_found:
                print(f"[Server message ({time.strftime('%H:%M:%S')})] {username} is not in {channel_name}.", flush=True)
                return
            return
    
    print(f"[Server message ({time.strftime('%H:%M:%S')})] {channel_name} does not exist.", flush=True)
    return

def empty(command, channels) -> None:
    """
    Implement /empty function
    Status: TODO
    Args:
        command (str): The command to empty a channel.
        channels (dict): A dictionary of all channels.
    """
    # Write your code here...
    # validate the command structure
    if (len(command.split()) != 2):
        return  
    empty_channel = command.split()[1]
    # check if the channel exists in the serverv
    for channel in channels.values():
        if channel.name == empty_channel:
            # if the channel exists, close connections of all clients in the channel
            sys.stdout.flush()
            for client in channel.clients:
                client.connection.send("EXIT".encode())
                client.connection.shutdown(socket.SHUT_RDWR)
                client.connection.close()
            channel.clients.clear()
            print(f"[Server message ({time.strftime('%H:%M:%S')})] {empty_channel} has been emptied.", flush=True)
            return
        
    print(f"[Server message ({time.strftime('%H:%M:%S')})] {empty_channel} does not exist.", flush=True)
    return

def mute_user(command, channels) -> None:
    """
    Implement /mute function
    Status: TODO
    Args:
        command (str): The command to mute a user in a channel.
        channels (dict): A dictionary of all channels.
    """
    # Write your code here...
    # validate the command structure
    if (len(command.split()) != 4):
        return  
    channel_name = command.split()[1]
    username = command.split()[2]
    mute_time = command.split()[3]
    found_user = False

    # if user is not in the channel, print error message 
    for channel in channels.values():
        if channel.name == channel_name:
            user_channel = channel
            for client in channel.clients:
                if client.username == username:
                    found_user = True
                    break
            break
            
    
    if found_user == False:
        print(f"[Server message ({time.strftime('%H:%M:%S')})] {username} is not here.", flush=True)
    
                
    # check if the mute time is valid
    if not mute_time.isdigit() or int(mute_time) < 1:
        print(f"[Server message ({time.strftime('%H:%M:%S')})] Invalid mute time.", flush=True)
        return
    
    if found_user:
        # if user is in the channel, mute it and send messages to all clients
        client.muted = True
        client.mute_duration = int(mute_time)
        sys.stdout.flush()
        print(f"[Server message ({time.strftime('%H:%M:%S')})] Muted {username} for {mute_time} seconds.", flush=True)
        client.connection.send(f"[Server message ({time.strftime('%H:%M:%S')})] You have been muted for {mute_time} seconds.".encode())
        server_broadcast(user_channel, f"{username} has been muted for {mute_time} seconds.", muted = username)
        return
    return
                
    
def shutdown(channels) -> None:
    """
    Implement /shutdown function
    Status: TODO
    Args:
        channels (dict): A dictionary of all channels.
    """
    # Write your code here...
    # close connections of all clients in all channels and exit the server
    sys.stdout.flush()
    for channel in channels.values():
        for client in channel.clients:
            client.connection.send("EXIT".encode())
            client.connection.shutdown(socket.SHUT_RDWR)
            client.connection.close()
        for queued_client in channel.queue.queue:
            queued_client.connection.send("EXIT".encode())
            queued_client.connection.shutdown(socket.SHUT_RDWR)
            queued_client.connection.close()
    # end of code insertion, keep the os._exit(0) as it is
    os._exit(0)

def server_commands(channels) -> None:
    """
    Implement commands to kick a user, empty a channel, mute a user, and shutdown the server.
    Each command has its own validation and error handling. 
    Status: Given
    Args:
        channels (dict): A dictionary of all channels.
    Returns:
        None
    """
    while True:
        try:
            command = input()
            if command.startswith('/kick'):
                kick_user(command, channels)
            elif command.startswith("/empty"):
                empty(command, channels)
            elif command.startswith("/mute"):
                mute_user(command, channels)
            elif command == "/shutdown":
                shutdown(channels)
            else:
                continue
        except EOFError:
            continue
        except Exception as e:
            print(f"{e}")
            sys.exit(1)

def check_inactive_clients(channels) -> None:
    """
    Continuously manages clients in all channels. Checks if a client is muted, in queue, or has run out of time. 
    If a client's time is up, they are removed from the channel and their connection is closed. 
    A server message is sent to all clients in the channel. The function also handles EOFError exceptions.
    Status: TODO
    Args:
        channels (dict): A dictionary of all channels.
    """
    # Write your code here...
    # parse through all the clients in all the channels
    while True:
        try:
            for channel_name in channels:
                channel = channels[channel_name]
                for client in channel.clients:
                    # if client is muted or in queue, do nothing
                    if client.muted or client.in_queue:
                        continue
                    # remove client from the channel and close connection, print AFK message
                    if client.remaining_time <= 0:
                        channel.clients.remove(client)
                        client.connection.send("EXIT".encode())
                        client.connection.shutdown(socket.SHUT_RDWR)
                        client.connection.close()
                        print(f"[Server message ({time.strftime('%H:%M:%S')})] {client.username} went AFK.", flush=True)
                        server_broadcast(channel, f"{client.username} went AFK.")
                    # if client is not muted, decrement remaining time
                    else:
                        client.remaining_time -= 1
            time.sleep(0.99)
        except EOFError:
            continue

def handle_mute_durations(channels) -> None:
    """
    Continuously manages the mute status of clients in all channels. If a client's mute duration has expired, 
    their mute status is lifted. If a client is still muted, their mute duration is decremented. 
    The function sleeps for 0.99 seconds between iterations and handles EOFError exceptions.
    Status: Given
    Args:
        channels (dict): A dictionary of all channels.
    """
    while True:
        try:
            for channel_name in channels:
                channel = channels[channel_name]
                for client in channel.clients:
                    if client.mute_duration <= 0:
                        client.muted = False
                        client.mute_duration = 0
                    if client.muted and client.mute_duration > 0:
                        client.mute_duration -= 1
            time.sleep(0.99)
        except EOFError:
            continue

def main():
    try:
        if len(sys.argv) != 2:
            print("Usage: python3 chatserver.py configfile")
            sys.exit(1)

        config_file = sys.argv[1]

        # parsing and creating channels
        parsed_lines = parse_config(config_file)
        channels = get_channels_dictionary(parsed_lines)

        # creating individual threads to handle channels connections
        for _, channel in channels.items():
            thread = threading.Thread(target=channel_handler, args=(channel, channels))
            thread.start()

        server_commands_thread = threading.Thread(target=server_commands, args=(channels,))
        server_commands_thread.start()

        inactive_clients_thread = threading.Thread(target=check_inactive_clients, args=(channels,))
        inactive_clients_thread.start()

        mute_duration_thread = threading.Thread(target=handle_mute_durations, args=(channels,))
        mute_duration_thread.start()
    except KeyboardInterrupt:
        print("Ctrl + C Pressed. Exiting...")
        os._exit(0)


if __name__ == "__main__":
    main()
