'''
Script for publishing control messages to a
RabbitMQ Pipeline

Tech: 
GUI: PySimpleGUi
 https://www.pysimplegui.org/en/latest/

Backend: pika
 https://pika.readthedocs.io/en/stable/
 
'''

import PySimpleGUI as sg
import sys
import json
import pika

# sets theme, to see a list of all possible themes use the following command: print(sg.theme_list)
sg.theme('SystemDefault')
# sets font and font size
sg.set_options(font="Franklin 12")

# default routing_key = control
routing_key = ""

# creates window and allows user to publish control messages to RabbitMQ

def main():
    commands = ["shutdown", "suspend", "resume", "mute", "unmute", "loglevel",
                "start_mountpoint", "stop_mountpoint", "print_mountpoints", "print_environment"]

    at_commands = ["shutdown", "suspend", "resume", "mute", "unmute"]

    # txtColumn and inpColumn are the layout for the window
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
        [sg.InputText(size=inpFieldSize, key="HOST",
                      default_text="localhost")],
        [sg.InputText(size=inpFieldSize, key="PORT", default_text="")],
        [sg.InputText(size=inpFieldSize)],
        [sg.InputText(size=inpFieldSize)],
        [sg.InputText(size=inpFieldSize, key="EXCH",
                      default_text="amq.direct")],
        [sg.Button("CONNECT", size=(30, 1))],
        [sg.Button("DISCONNECT", size=(30, 1), disabled=True)],
        [sg.InputText(size=inpFieldSize, key="recipients")],
        [sg.InputText(size=inpFieldSize, key="sender")],
        [sg.Combo(commands, size=inpFieldSize, key="command")],
        [sg.InputText(size=inpFieldSize, key="arguments")],
        [sg.Button("PUBLISH", size=(30, 1), key="PUBLISH", disabled=True)]
    ], expand_y=True)

    # opens window and checks for events
    window = sg.Window('RabbitMQcontrol', [
                       [txtColumn, inpColumn]])  # titel und layout
    connection = None
    channel = None
    while True:
        # following if statements all check for events
        event, values = window.read()
        if event == "Exit" or event == sg.WIN_CLOSED:  # if user closes window or clicks cancel
            window.close()

        if event == "CONNECT":
            window['CONNECT'].update(disabled=True)
            window['DISCONNECT'].update(disabled=False)

            window['PUBLISH'].update(disabled=False)

            # connection to the RabbitMQ Host
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=values["HOST"], port=values["PORT"]))

            channel = connection.channel()
            print("Channel is open: ", channel.is_open)

        if event == "DISCONNECT":
            window['CONNECT'].update(disabled=False)
            window['DISCONNECT'].update(disabled=True)
            window['PUBLISH'].update(disabled=True)

            if connection is not None:
                connection.close()
                print("Connection closed")

        if event == "Exit" or event == sg.WIN_CLOSED:
            window.close()
            break

        if event == "PUBLISH":
            # json format handling by changing the values of the keys in the dict values
            values["recipients"] = values["recipients"].split(",")

            if values["command"] in at_commands:
                values["arguments"] = {"at": values["arguments"]}

            elif values["command"] == "start_mountpoint" or values["command"] == "stop_mountpoint":
                values["arguments"] = {"mountpoint": values["arguments"]}

            elif values["command"] == "loglevel":
                values["arguments"] = {"level": values["arguments"]}

            v = dict(list(values.items())[5:])

            jsonData = json.dumps(v)
            if channel:
                channel.basic_publish(
                    exchange=values["EXCH"], routing_key=routing_key, body=jsonData)
                print(" [x] Sent json")

            else:
                print("No channel was opened")

    # makes sure channel/connection is open before closing
    if channel is not None:
        channel.close()

    if connection is not None:
        connection.close()

    sys.exit()


if __name__ == "__main__":
    main()
