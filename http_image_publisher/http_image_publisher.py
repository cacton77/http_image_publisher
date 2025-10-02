#!/usr/bin/env python3

import rclpy
from rclpy.node import Node
from sensor_msgs.msg import Image, CameraInfo
from cv_bridge import CvBridge
import cv2
import requests
import numpy as np
import time
import threading
from enum import Enum
from queue import Queue, Empty
from PIL import Image as PILImage
from io import BytesIO


class ConnectionState(Enum):
    DISCONNECTED = 1
    CONNECTING = 2
    CONNECTED = 3


class HttpImagePublisher(Node):
    def __init__(self):
        super().__init__('http_image_publisher')

        # Parameters
        self.declare_parameter('stream_url', 'http://example.com/stream.mjpg')
        self.declare_parameter('base_topic', 'camera')
        self.declare_parameter('frame_id', 'camera_frame')
        self.declare_parameter('connection_timeout', 5.0)
        self.declare_parameter('use_opencv_for_mjpeg', True)
        self.declare_parameter('use_pillow', True)

        # Camera calibration parameters
        self.declare_parameter('camera_name', 'http_camera')
        self.declare_parameter('image_width', 640)
        self.declare_parameter('image_height', 480)
        self.declare_parameter(
            'camera_matrix', [800.0, 0.0, 320.0, 0.0, 800.0, 240.0, 0.0, 0.0, 1.0])
        self.declare_parameter('distortion_coefficients', [
                               0.0, 0.0, 0.0, 0.0, 0.0])

        # Get parameters
        self.stream_url = self.get_parameter('stream_url').value
        self.base_topic = self.get_parameter('base_topic').value
        self.frame_id = self.get_parameter('frame_id').value
        self.connection_timeout = self.get_parameter(
            'connection_timeout').value
        self.use_opencv_for_mjpeg = self.get_parameter(
            'use_opencv_for_mjpeg').value
        self.use_pillow = self.get_parameter('use_pillow').value

        # Camera info parameters
        self.camera_name = self.get_parameter('camera_name').value
        self.image_width = self.get_parameter('image_width').value
        self.image_height = self.get_parameter('image_height').value
        self.camera_matrix = self.get_parameter('camera_matrix').value
        self.distortion_coeffs = self.get_parameter(
            'distortion_coefficients').value

        # CV bridge
        self.bridge = CvBridge()

        # Publishers
        self.image_pub = self.create_publisher(
            Image, f'{self.base_topic}/image_raw', 10)
        self.camera_info_pub = self.create_publisher(
            CameraInfo, f'{self.base_topic}/camera_info', 10)

        # Connection state management
        self.connection_state = ConnectionState.DISCONNECTED
        self.connection_lock = threading.Lock()
        self.shutdown_requested = False

        # Stream handling
        self.cap = None
        self.stream_session = None
        self.is_mjpeg_stream = False
        self.use_pillow_method = False
        self.actual_width = self.image_width
        self.actual_height = self.image_height

        # Frame grabbing thread
        self.frame_queue = Queue(maxsize=1)
        self.frame_thread = None
        self.frame_thread_active = False

        # Frame statistics
        self.frames_received = 0
        self.frames_published = 0
        self.last_stats_time = time.time()
        self.grab_times = []
        self.publish_times = []
        self.stats_timer = self.create_timer(10.0, self.print_stats)

        # Initialize camera info message
        self.setup_camera_info()

        # Publishing timer - runs fast to publish frames as soon as available
        self.publish_timer = self.create_timer(0.001, self.publish_callback)

        # Start initial connection
        self.start_connection()

        self.get_logger().info(
            f'Low-latency HTTP Image Publisher initialized for {self.stream_url}')
        self.get_logger().info(
            f'Publishing to {self.base_topic}/image_raw (continuous mode)')
        self.get_logger().info(
            f'Using Pillow: {self.use_pillow}, OpenCV for MJPEG: {self.use_opencv_for_mjpeg}')

    def setup_camera_info(self):
        """Setup camera info message with calibration data"""
        self.camera_info_msg = CameraInfo()
        self.camera_info_msg.header.frame_id = self.frame_id

        # Image dimensions
        self.camera_info_msg.width = self.image_width
        self.camera_info_msg.height = self.image_height

        # Camera matrix (3x3)
        self.camera_info_msg.k = self.camera_matrix

        # Distortion coefficients
        self.camera_info_msg.d = self.distortion_coeffs
        self.camera_info_msg.distortion_model = "plumb_bob"

        # Rectification matrix (identity for monocular camera)
        self.camera_info_msg.r = [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0]

        # Projection matrix (3x4)
        self.camera_info_msg.p = [
            self.camera_matrix[0], 0.0, self.camera_matrix[2], 0.0,
            0.0, self.camera_matrix[4], self.camera_matrix[5], 0.0,
            0.0, 0.0, 1.0, 0.0
        ]

    def start_connection(self):
        """Start the initial connection attempt"""
        with self.connection_lock:
            if self.connection_state == ConnectionState.CONNECTING:
                return

            self.connection_state = ConnectionState.CONNECTING
            self.get_logger().info('Starting connection to stream...')

        connection_thread = threading.Thread(target=self._attempt_connection)
        connection_thread.daemon = True
        connection_thread.start()

    def _attempt_connection(self):
        """Attempt to establish connection to the stream"""
        try:
            self.get_logger().info(f'Connecting to {self.stream_url}')

            # Check stream type
            test_response = requests.head(
                self.stream_url, timeout=self.connection_timeout)
            content_type = test_response.headers.get('content-type', '')
            self.get_logger().info(f'Content-Type: {content_type}')

            success = False

            if 'multipart' in content_type and self.use_opencv_for_mjpeg:
                # OpenCV for MJPEG streams
                self.get_logger().info('Using OpenCV VideoCapture for MJPEG stream')
                cap = cv2.VideoCapture(self.stream_url)

                # Configure for lowest latency
                cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)

                if cap.isOpened():
                    # Clear any buffered frames
                    for _ in range(5):
                        cap.grab()

                    # Test read
                    ret, frame = cap.read()
                    if ret and frame is not None:
                        with self.connection_lock:
                            if self.cap:
                                self.cap.release()
                            self.cap = cap
                            self.actual_height, self.actual_width = frame.shape[:2]
                            self.update_camera_info_dimensions()
                            self.is_mjpeg_stream = True
                            self.use_pillow_method = False
                            success = True
                        self.get_logger().info(
                            f'OpenCV connection successful - {self.actual_width}x{self.actual_height}')
                    else:
                        cap.release()
                else:
                    cap.release()

            if not success and self.use_pillow:
                # Try Pillow method for single images
                self.get_logger().info('Using Pillow for image fetching')
                session = requests.Session()

                # Test the connection
                test_response = session.get(
                    self.stream_url, timeout=self.connection_timeout, stream=True)
                test_response.raise_for_status()

                # Test decode with Pillow
                pil_image = PILImage.open(BytesIO(test_response.content))
                pil_image.load()  # Force load to ensure it's valid

                test_response.close()

                with self.connection_lock:
                    if self.stream_session:
                        self.stream_session.close()
                    self.stream_session = session
                    self.is_mjpeg_stream = False
                    self.use_pillow_method = True
                    success = True
                self.get_logger().info('Pillow connection successful')

            elif not success:
                # Fallback to requests + OpenCV decode
                self.get_logger().info('Using requests + OpenCV decode method')
                session = requests.Session()

                test_response = session.get(
                    self.stream_url, timeout=self.connection_timeout, stream=True)
                test_response.raise_for_status()
                test_response.close()

                with self.connection_lock:
                    if self.stream_session:
                        self.stream_session.close()
                    self.stream_session = session
                    self.is_mjpeg_stream = False
                    self.use_pillow_method = False
                    success = True
                self.get_logger().info('Requests + OpenCV connection successful')

            if success:
                with self.connection_lock:
                    self.connection_state = ConnectionState.CONNECTED

                # Start frame grabbing thread
                self.start_frame_thread()
                self.get_logger().info('Stream connection established successfully')
            else:
                raise Exception(
                    "Failed to establish connection with any method")

        except Exception as e:
            self.get_logger().error(f'Connection attempt failed: {e}')
            with self.connection_lock:
                self.connection_state = ConnectionState.DISCONNECTED

    def start_frame_thread(self):
        """Start the continuous frame grabbing thread"""
        if self.frame_thread and self.frame_thread.is_alive():
            return

        self.frame_thread_active = True
        self.frame_thread = threading.Thread(target=self._frame_grabbing_loop)
        self.frame_thread.daemon = True
        self.frame_thread.start()
        self.get_logger().info('Frame grabbing thread started')

    def stop_frame_thread(self):
        """Stop the frame grabbing thread"""
        self.frame_thread_active = False
        if self.frame_thread:
            self.frame_thread.join(timeout=2.0)
        self.get_logger().info('Frame grabbing thread stopped')

    def _frame_grabbing_loop(self):
        """Continuously grab frames and put latest in queue"""
        consecutive_failures = 0
        max_failures = 30

        while self.frame_thread_active and not self.shutdown_requested:
            try:
                grab_start = time.time()

                with self.connection_lock:
                    cap = self.cap
                    session = self.stream_session
                    state = self.connection_state
                    use_pillow = self.use_pillow_method

                if state != ConnectionState.CONNECTED:
                    time.sleep(0.1)
                    continue

                frame = None

                if cap is not None:
                    # OpenCV MJPEG stream - aggressively clear buffer
                    for _ in range(3):
                        cap.grab()
                    ret, frame = cap.retrieve()

                    if not ret or frame is None:
                        consecutive_failures += 1
                        if consecutive_failures >= max_failures:
                            self.get_logger().error('Too many consecutive frame failures - connection lost')
                            self._handle_connection_loss()
                            break
                        continue

                    consecutive_failures = 0

                elif session is not None:
                    if use_pillow:
                        frame = self.get_single_image_pillow(session)
                    else:
                        frame = self.get_single_image_opencv(session)

                    if frame is None:
                        consecutive_failures += 1
                        if consecutive_failures >= max_failures:
                            self.get_logger().error('Too many consecutive frame failures - connection lost')
                            self._handle_connection_loss()
                            break
                        continue

                    consecutive_failures = 0

                    # Update dimensions on first frame
                    if self.actual_width == self.image_width and self.actual_height == self.image_height:
                        self.actual_height, self.actual_width = frame.shape[:2]
                        self.update_camera_info_dimensions()

                # Put frame in queue
                if frame is not None and isinstance(frame, np.ndarray) and frame.size > 0:
                    if len(frame.shape) == 3 and frame.shape[2] == 3:
                        # Clear queue and put latest frame
                        while not self.frame_queue.empty():
                            try:
                                self.frame_queue.get_nowait()
                            except Empty:
                                break

                        try:
                            self.frame_queue.put_nowait(frame)
                            self.frames_received += 1

                            # Track timing
                            grab_time = time.time() - grab_start
                            self.grab_times.append(grab_time)
                            if len(self.grab_times) > 100:
                                self.grab_times.pop(0)
                        except:
                            pass

            except Exception as e:
                self.get_logger().error(f'Error in frame grabbing loop: {e}')
                self._handle_connection_loss()
                break

        self.get_logger().info('Frame grabbing loop exited')

    def publish_callback(self):
        """Fast callback to publish frames as soon as they're available"""
        try:
            publish_start = time.time()

            # Non-blocking get
            frame = self.frame_queue.get_nowait()
            self.publish_image_and_info(frame)
            self.frames_published += 1

            # Track timing
            publish_time = time.time() - publish_start
            self.publish_times.append(publish_time)
            if len(self.publish_times) > 100:
                self.publish_times.pop(0)
        except Empty:
            pass
        except Exception as e:
            self.get_logger().error(f'Error in publish callback: {e}')

    def get_single_image_pillow(self, session):
        """Fetch a single image from the URL using Pillow"""
        try:
            response = session.get(
                self.stream_url, timeout=self.connection_timeout)
            response.raise_for_status()

            if not response.content:
                return None

            # Decode with Pillow
            pil_image = PILImage.open(BytesIO(response.content))

            # Convert to RGB if necessary
            if pil_image.mode != 'RGB':
                pil_image = pil_image.convert('RGB')

            # Convert to numpy array (RGB format)
            frame_rgb = np.array(pil_image, dtype=np.uint8)

            # Convert RGB to BGR for ROS compatibility
            frame_bgr = cv2.cvtColor(frame_rgb, cv2.COLOR_RGB2BGR)

            return frame_bgr

        except Exception as e:
            self.get_logger().debug(f'Error fetching image with Pillow: {e}')
            return None

    def get_single_image_opencv(self, session):
        """Fetch a single image from the URL using OpenCV decode"""
        try:
            response = session.get(
                self.stream_url, timeout=self.connection_timeout)
            response.raise_for_status()

            if not response.content:
                return None

            nparr = np.frombuffer(response.content, np.uint8)
            if nparr.size == 0:
                return None

            frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            return frame

        except Exception as e:
            self.get_logger().debug(f'Error fetching image with OpenCV: {e}')
            return None

    def publish_image_and_info(self, cv_image):
        """Publish both image and camera info with synchronized timestamps"""
        try:
            if cv_image.size == 0 or len(cv_image.shape) != 3:
                return

            # Create timestamp
            timestamp = self.get_clock().now().to_msg()

            # Create and publish image message
            ros_image = self.bridge.cv2_to_imgmsg(cv_image, encoding='bgr8')
            ros_image.header.stamp = timestamp
            ros_image.header.frame_id = self.frame_id
            self.image_pub.publish(ros_image)

            # Update and publish camera info
            self.camera_info_msg.header.stamp = timestamp
            self.camera_info_pub.publish(self.camera_info_msg)

        except Exception as e:
            self.get_logger().error(f'Image publishing error: {e}')

    def update_camera_info_dimensions(self):
        """Update camera info with actual image dimensions"""
        if self.actual_width != self.image_width or self.actual_height != self.image_height:
            scale_x = self.actual_width / self.image_width
            scale_y = self.actual_height / self.image_height

            self.camera_info_msg.width = self.actual_width
            self.camera_info_msg.height = self.actual_height

            scaled_matrix = list(self.camera_matrix)
            scaled_matrix[0] *= scale_x
            scaled_matrix[2] *= scale_x
            scaled_matrix[4] *= scale_y
            scaled_matrix[5] *= scale_y

            self.camera_info_msg.k = scaled_matrix

            self.camera_info_msg.p = [
                scaled_matrix[0], 0.0, scaled_matrix[2], 0.0,
                0.0, scaled_matrix[4], scaled_matrix[5], 0.0,
                0.0, 0.0, 1.0, 0.0
            ]

    def _handle_connection_loss(self):
        """Handle loss of connection"""
        self.stop_frame_thread()

        with self.connection_lock:
            self.connection_state = ConnectionState.DISCONNECTED

            if self.cap:
                self.cap.release()
                self.cap = None
            if self.stream_session:
                self.stream_session.close()
                self.stream_session = None

        self.get_logger().error('Connection lost - node stopping')

    def print_stats(self):
        """Print connection statistics"""
        current_time = time.time()
        time_elapsed = current_time - self.last_stats_time

        grabbed_fps = self.frames_received / time_elapsed if time_elapsed > 0 else 0
        published_fps = self.frames_published / time_elapsed if time_elapsed > 0 else 0
        queue_size = self.frame_queue.qsize()

        with self.connection_lock:
            state = self.connection_state

        # Calculate average times
        avg_grab_time = sum(self.grab_times) / \
            len(self.grab_times) if self.grab_times else 0
        avg_publish_time = sum(self.publish_times) / \
            len(self.publish_times) if self.publish_times else 0

        self.get_logger().info(
            f'Stats: State={state.name}, Grabbed={grabbed_fps:.1f}fps, '
            f'Published={published_fps:.1f}fps, QueueSize={queue_size}')
        self.get_logger().info(
            f'Timing: AvgGrab={avg_grab_time*1000:.1f}ms, AvgPublish={avg_publish_time*1000:.1f}ms')

        self.frames_received = 0
        self.frames_published = 0
        self.last_stats_time = current_time

    def destroy_node(self):
        """Clean up resources when node is destroyed"""
        self.shutdown_requested = True
        self.stop_frame_thread()

        with self.connection_lock:
            if self.cap:
                self.cap.release()
            if self.stream_session:
                self.stream_session.close()

        super().destroy_node()


def main(args=None):
    rclpy.init(args=args)
    node = HttpImagePublisher()

    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
