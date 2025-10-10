#!/usr/bin/env python3

import rclpy
from rclpy.node import Node
from sensor_msgs.msg import Image
from cv_bridge import CvBridge
import cv2 as cv
import threading


class MjpegStreamPublisher(Node):
    def __init__(self):
        super().__init__('mjpeg_stream_publisher')

        # Declare parameters
        self.declare_parameter(
            'stream_url', 'http://localhost:8080/stream.mjpg')
        self.declare_parameter('topic_name', 'camera/image_raw')
        self.declare_parameter('frame_rate', 30.0)

        # Get parameters
        self.stream_url = self.get_parameter('stream_url').value
        topic_name = self.get_parameter('topic_name').value
        frame_rate = self.get_parameter('frame_rate').value

        # Create publisher
        self.publisher = self.create_publisher(Image, topic_name, 10)

        # Initialize CV Bridge
        self.bridge = CvBridge()

        # Stream control
        self.running = True
        self.stream_thread = threading.Thread(target=self.stream_frames)
        self.stream_thread.daemon = True
        self.stream_thread.start()

        self.get_logger().info(f'MJPEG Stream Publisher started')
        self.get_logger().info(f'Stream URL: {self.stream_url}')
        self.get_logger().info(f'Publishing to: {topic_name}')

    def stream_frames(self):
        """Continuously read frames from MJPEG stream"""
        # Open video capture
        cap = cv.VideoCapture(self.stream_url)

        if not cap.isOpened():
            self.get_logger().error("Cannot open camera stream")
            return

        while self.running:
            # Capture frame-by-frame
            ret, frame = cap.read()

            # if frame is read correctly ret is True
            if not ret:
                self.get_logger().warn("Can't receive frame (stream end?)")
                break

            try:
                # Convert to ROS Image message
                # frame is already in BGR format from cv2
                msg = self.bridge.cv2_to_imgmsg(frame, encoding='bgr8')
                msg.header.stamp = self.get_clock().now().to_msg()
                msg.header.frame_id = 'eoat_camera_link'

                # Publish
                self.publisher.publish(msg)

            except Exception as e:
                self.get_logger().warn(f'Error processing frame: {str(e)}')

        # When everything done, release the capture
        cap.release()

    def destroy_node(self):
        """Clean shutdown"""
        self.running = False
        if self.stream_thread.is_alive():
            self.stream_thread.join(timeout=2.0)
        super().destroy_node()


def main(args=None):
    rclpy.init(args=args)
    node = MjpegStreamPublisher()

    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
