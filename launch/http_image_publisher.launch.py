# launch/camera_with_transport.launch.py
from launch import LaunchDescription
from launch_ros.actions import Node

from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration


def generate_launch_description():
    declared_arguments = [
        DeclareLaunchArgument(
            "stream_url", default_value='http://192.168.0.92:5000/video_feed', description='URL of the video stream'),
        DeclareLaunchArgument(
            "base_topic", default_value='camera', description='Base topic for the camera'),
        DeclareLaunchArgument(
            "frame_id", default_value='camera_frame', description='Frame ID for the camera'),
        DeclareLaunchArgument("publish_rate", default_value='30.0',
                              description='Publish rate for the camera'),
        DeclareLaunchArgument("connection_timeout", default_value='5.0',
                              description='Connection timeout for the stream')
    ]
    return LaunchDescription(declared_arguments + [
        # HTTP Image Publisher
        Node(
            package='http_image_publisher',
            executable='http_image_publisher',
            name='camera_publisher',
            parameters=[{
                'stream_url': LaunchConfiguration('stream_url'),
                'base_topic': LaunchConfiguration('base_topic'),
                'frame_id': LaunchConfiguration('frame_id'),
                'publish_rate': LaunchConfiguration('publish_rate'),
                'connection_timeout': LaunchConfiguration('connection_timeout'),
            }],
            # arguments=['--ros-args', '--log-level', 'DEBUG']
        ),

        # Compressed republisher
        Node(
            package='image_transport',
            executable='republish',
            name='compressed_republisher',
            arguments=['raw', 'compressed'],
            remappings=[
                ('in', 'camera/image_raw'),
                ('out/compressed', 'camera/image_raw/compressed')
            ]
        ),

        # Theora republisher with explicit parameters
        Node(
            package='image_transport',
            executable='republish',
            name='theora_republisher',
            arguments=['raw', 'theora'],
            parameters=[{
                'theora.optimize_for': 0,
                'theora.quality': 40,
                'theora.bitrate': 800000,
                'theora.keyframe_frequency': 30
            }],
            remappings=[
                ('in', 'camera/image_raw'),
                ('out/theora', 'camera/image_raw/theora')
            ]
        )
    ])
