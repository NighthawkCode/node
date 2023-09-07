# pynode

`pynode` is a Python wrapper around node. It provides a comprehensive set of python interface to the node ecosystem, allowing to query the topics being published and subscribe to receive topics.

Created by Lawrence Ibarria at Verdant Robotics, [lawrence@verdantrobotics.com](mailto:lawrence@verdantrobotics.com).

The wrapper provides a set of module functions.

## num_topics

This function will return the number of topics in the system. It takes a parameter, the IP of nodemaster (the vehicle tablet).

```python
> import pynode
> pynode.num_topics("10.200.200.8")
  10
```

# get_all_topics

This function returns a python list of topics. Each topic contains the name, message type, topic type, and even the whole cbuf string for decoding.

```python
> import pynode
> l = pynode.get_all_topics("10.200.200.8")
> for tp in l:
...   print(f"Topic {tp.topic_name} type {tp.type} message {tp.msg_name}")

    Topic navbox/pose_window type 1 message messages::vehicle_pose_window
    Topic tablet/change_log_level type 1 message messages::set_loglevel
    Topic tablet/vdash_log type 3 message messages::logmsg
    Topic tablet/recording_control type 1 message messages::recording_control
    Topic tablet/turret_spray_pattern_viz type 1 message messages::spray_pattern_command
    Topic tablet/spraybox0/turret_control type 1 message messages::turret_control
    Topic tablet/spraybox1/turret_control type 1 message messages::turret_control
    Topic tablet/spraybox2/turret_control type 1 message messages::turret_control
    Topic tablet/spraybox3/turret_control type 1 message messages::turret_control
    Topic tablet/spraybox4/turret_control type 1 message messages::turret_control
    Topic tablet/spraybox5/turret_control type 1 message messages::turret_control
    Topic tablet/selected_trial type 1 message messages::selected_trial
    Topic tablet/set_syncbox_params type 1 message messages::syncbox_params
    Topic tablet/set_spray_mode type 1 message messages::spray_mode
    Topic tablet/fault_events.vdash type 1 message messages::fault_monitor_event
    Topic fault_events.vdash type 1 message messages::fault_monitor_event
```
