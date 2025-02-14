'''
Script for publishing control messages to a
Data Pipeline using RabbitMQ as the message broker.

Tech-Stack: 
GUI: PySimpleGUi
 https://www.pysimplegui.org/en/latest/

Backend: pika
 https://pika.readthedocs.io/en/stable/
 
'''

import PySimpleGUI as sg
import json
import pika

sg.theme('SystemDefault')
sg.set_options(font="Franklin 12")

routing_key = "test"

def main():
    commands = ["shutdown", "suspend", "resume", "mute", "unmute", "loglevel",
                "start_mountpoint", "stop_mountpoint", "print_mountpoints", "print_environment"]

    at_commands = ["shutdown", "suspend", "resume", "mute", "unmute"]

    inpFieldSize = (30, 9)
    txtColumn = sg.Column([
        [sg.Text('Host')],
        [sg.Text("Port")],
        [sg.Text('Username')],
        [sg.Text('Password')],
        [sg.Text('Exchange')],
        [sg.Text("", size=(10, 3), pad=(5, 8))],
        [sg.Text('Recipient')],
        [sg.Text('Sender')],
        [sg.Text('Command')],
        [sg.Text('Arguments')],
        [sg.Text("", size=(10, 1))]
    ], expand_y=True)

    inpColumn = sg.Column([
        [sg.InputText(size=inpFieldSize, key="HOST", default_text="localhost")],
        [sg.InputText(size=inpFieldSize, key="PORT", default_text="5672")],
        [sg.InputText(size=inpFieldSize)],
        [sg.InputText(size=inpFieldSize)],
        [sg.InputText(size=inpFieldSize, key="EXCH", default_text="amq.direct")],
        [sg.Button("CONNECT", size=(30, 1))],
        [sg.Button("DISCONNECT", size=(30, 1), disabled=True)],
        [sg.InputText(size=inpFieldSize, key="recipients")],
        [sg.InputText(size=inpFieldSize, key="sender")],
        [sg.Combo(commands, size=inpFieldSize, key="command")],
        [sg.InputText(size=inpFieldSize, key="arguments")],
        [sg.Button("PUBLISH", size=(30, 1), key="PUBLISH", disabled=True)]
    ], expand_y=True)

    window = sg.Window('RabbitMQcontrol', [[txtColumn, inpColumn]])
    connection = None
    channel = None
    
    while True:
        event, values = window.read()
        
        if event == "Exit" or event == sg.WIN_CLOSED:
            break

        if event == "CONNECT":
            try:
                # Convert port to integer
                port = int(values["PORT"]) if values["PORT"] else 5672
                
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=values["HOST"], port=port))
                channel = connection.channel()
                
                # Declare the queue and bind it to the exchange
                channel.queue_declare(queue=routing_key)
                channel.queue_bind(exchange=values["EXCH"], 
                                 queue=routing_key,
                                 routing_key=routing_key)
                
                print("Channel is open:", channel.is_open)
                window['CONNECT'].update(disabled=True)
                window['DISCONNECT'].update(disabled=False)
                window['PUBLISH'].update(disabled=False)
                
            except Exception as e:
                sg.popup_error(f'Connection error: {str(e)}')
                continue

        if event == "DISCONNECT":
            window['CONNECT'].update(disabled=False)
            window['DISCONNECT'].update(disabled=True)
            window['PUBLISH'].update(disabled=True)

            if connection is not None:
                connection.close()
                print("Connection closed")

        if event == "PUBLISH":
            try:
                values["recipients"] = values["recipients"].split(",")

                if values["command"] in at_commands:
                    values["arguments"] = {"at": values["arguments"]}
                elif values["command"] == "start_mountpoint" or values["command"] == "stop_mountpoint":
                    values["arguments"] = {"mountpoint": values["arguments"]}
                elif values["command"] == "loglevel":
                    values["arguments"] = {"level": values["arguments"]}

                v = dict(list(values.items())[5:])
                jsonData = json.dumps(v)
                
                if channel and channel.is_open:
                    channel.basic_publish(
                        exchange=values["EXCH"],
                        routing_key=routing_key,
                        body=jsonData)
                    print(" [x] Sent json:", jsonData)
                else:
                    sg.popup_error("No active channel. Please connect first.")
            
            except Exception as e:
                sg.popup_error(f'Publishing error: {str(e)}')

    window.close()
    
    if channel is not None and channel.is_open:
        channel.close()
    if connection is not None and connection.is_open:
        connection.close()

if __name__ == "__main__":
    main()