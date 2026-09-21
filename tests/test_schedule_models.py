"""
Tests for schedule-related Pydantic models.
"""

import unittest

import pytest

from nzovu import models
from nzovu.api.queueservice.v1 import request_response_pb2
from nzovu.api.schedule.v1 import schedule_pb2
from nzovu.utils import ResponseWrapper


@pytest.mark.skipif(not models.PYDANTIC_AVAILABLE, reason="Pydantic not installed")
class TestSchedulePydanticModels(unittest.TestCase):
    """Test Pydantic models for schedule operations."""

    def test_schedule_metadata_model(self):
        """Test ScheduleMetadata model creation."""
        metadata = models.ScheduleMetadata(
            payload={"task": "test"},
            state="SCHEDULED",
            cron_schedule="0 0 * * *",
            queue_name="test_queue",
            priority=5,
            timezone="America/New_York",
        )

        self.assertEqual(metadata.state, "SCHEDULED")
        self.assertEqual(metadata.cron_schedule, "0 0 * * *")
        self.assertEqual(metadata.queue_name, "test_queue")
        self.assertEqual(metadata.priority, 5)
        self.assertEqual(metadata.timezone, "America/New_York")

    def test_schedule_model(self):
        """Test Schedule model creation."""
        metadata = models.ScheduleMetadata(
            payload={"task": "test"},
            state="SCHEDULED",
            cron_schedule="0 0 * * *",
            queue_name="test_queue",
        )

        schedule = models.Schedule(schedule_id="test_schedule", metadata=metadata)

        self.assertEqual(schedule.schedule_id, "test_schedule")
        self.assertEqual(schedule.metadata.state, "SCHEDULED")
        self.assertEqual(schedule.metadata.queue_name, "test_queue")

    def test_schedule_from_proto(self):
        """Test Schedule.from_proto conversion."""
        # Create a proto schedule
        from google.protobuf.struct_pb2 import Struct

        from nzovu.api.common.v1 import common_pb2

        payload_struct = Struct()
        payload_struct.update({"task": "daily_report"})

        payload = common_pb2.Payload(data=payload_struct)

        metadata_proto = schedule_pb2.Schedule.Metadata(
            payload=payload,
            state=schedule_pb2.Schedule.Metadata.State.SCHEDULED,
            cron_schedule="0 0 * * *",
            queue_name="reports_queue",
            priority=5,
        )

        schedule_proto = schedule_pb2.Schedule(schedule_id="daily_schedule", metadata=metadata_proto)

        # Convert to Pydantic model
        schedule = models.Schedule.from_proto(schedule_proto)

        self.assertEqual(schedule.schedule_id, "daily_schedule")
        self.assertEqual(schedule.metadata.state, "SCHEDULED")
        self.assertEqual(schedule.metadata.cron_schedule, "0 0 * * *")
        self.assertEqual(schedule.metadata.queue_name, "reports_queue")
        self.assertEqual(schedule.metadata.priority, 5)
        self.assertEqual(schedule.metadata.payload, {"task": "daily_report"})

    def test_create_schedule_response_from_proto(self):
        """Test CreateScheduleResponse.from_proto conversion."""
        response_proto = request_response_pb2.CreateScheduleResponse(success=True)

        # Convert to Pydantic model
        response = models.CreateScheduleResponse.from_proto(response_proto)

        self.assertTrue(response.success)

    def test_get_schedule_response_from_proto(self):
        """Test GetScheduleResponse.from_proto conversion."""
        from google.protobuf.struct_pb2 import Struct

        from nzovu.api.common.v1 import common_pb2

        payload_struct = Struct()
        payload_struct.update({"data": "value"})
        payload = common_pb2.Payload(data=payload_struct)

        metadata_proto = schedule_pb2.Schedule.Metadata(
            payload=payload,
            state=schedule_pb2.Schedule.Metadata.State.PAUSED,
            cron_schedule="0 0 * * *",
            queue_name="my_queue",
        )

        schedule_proto = schedule_pb2.Schedule(schedule_id="my_schedule", metadata=metadata_proto)

        response_proto = request_response_pb2.GetScheduleResponse(schedule=schedule_proto)

        # Convert to Pydantic model
        response = models.GetScheduleResponse.from_proto(response_proto)

        self.assertIsNotNone(response.schedule)
        self.assertEqual(response.schedule.schedule_id, "my_schedule")
        self.assertEqual(response.schedule.metadata.state, "PAUSED")

    def test_list_schedules_response_from_proto(self):
        """Test ListSchedulesResponse.from_proto conversion."""
        from google.protobuf.struct_pb2 import Struct

        from nzovu.api.common.v1 import common_pb2

        # Create multiple schedules
        schedules = []
        for i in range(3):
            payload_struct = Struct()
            payload_struct.update({"index": i})
            payload = common_pb2.Payload(data=payload_struct)

            metadata_proto = schedule_pb2.Schedule.Metadata(
                payload=payload,
                state=schedule_pb2.Schedule.Metadata.State.SCHEDULED,
                cron_schedule=f"0 {i} * * *",
                queue_name=f"queue_{i}",
            )

            schedule_proto = schedule_pb2.Schedule(schedule_id=f"schedule_{i}", metadata=metadata_proto)
            schedules.append(schedule_proto)

        response_proto = request_response_pb2.ListSchedulesResponse(schedules=schedules)

        # Convert to Pydantic model
        response = models.ListSchedulesResponse.from_proto(response_proto)

        self.assertEqual(len(response.schedules), 3)
        self.assertEqual(response.schedules[0].schedule_id, "schedule_0")
        self.assertEqual(response.schedules[1].schedule_id, "schedule_1")
        self.assertEqual(response.schedules[2].schedule_id, "schedule_2")

    def test_delete_schedule_response_from_proto(self):
        """Test DeleteScheduleResponse.from_proto conversion."""
        response_proto = request_response_pb2.DeleteScheduleResponse()

        # Convert to Pydantic model
        response = models.DeleteScheduleResponse.from_proto(response_proto)

        self.assertTrue(response.success)

    def test_pause_schedule_response_from_proto(self):
        """Test PauseScheduleResponse.from_proto conversion."""
        response_proto = request_response_pb2.PauseScheduleResponse()

        # Convert to Pydantic model
        response = models.PauseScheduleResponse.from_proto(response_proto)

        self.assertTrue(response.success)

    def test_resume_schedule_response_from_proto(self):
        """Test ResumeScheduleResponse.from_proto conversion."""
        response_proto = request_response_pb2.ResumeScheduleResponse()

        # Convert to Pydantic model
        response = models.ResumeScheduleResponse.from_proto(response_proto)

        self.assertTrue(response.success)

    def test_response_wrapper_to_model_with_schedule(self):
        """Test ResponseWrapper.to_model() with schedule responses."""
        response_proto = request_response_pb2.CreateScheduleResponse(success=True)

        # Wrap and convert
        wrapper = ResponseWrapper(response_protobuf=response_proto)
        response = wrapper.to_model()

        # Should auto-detect and return CreateScheduleResponse
        self.assertIsInstance(response, models.CreateScheduleResponse)
        self.assertTrue(response.success)

    def test_response_wrapper_to_model_with_list_schedules(self):
        """Test ResponseWrapper.to_model() with ListSchedulesResponse."""
        from google.protobuf.struct_pb2 import Struct

        from nzovu.api.common.v1 import common_pb2

        payload_struct = Struct()
        payload_struct.update({"task": "test"})
        payload = common_pb2.Payload(data=payload_struct)

        metadata_proto = schedule_pb2.Schedule.Metadata(
            payload=payload,
            state=schedule_pb2.Schedule.Metadata.State.SCHEDULED,
            cron_schedule="0 0 * * *",
            queue_name="test_queue",
        )

        schedule_proto = schedule_pb2.Schedule(schedule_id="test_schedule", metadata=metadata_proto)

        response_proto = request_response_pb2.ListSchedulesResponse(schedules=[schedule_proto])

        # Wrap and convert
        wrapper = ResponseWrapper(response_protobuf=response_proto)
        response = wrapper.to_model()

        # Should auto-detect and return ListSchedulesResponse
        self.assertIsInstance(response, models.ListSchedulesResponse)
        self.assertEqual(len(response.schedules), 1)
        self.assertEqual(response.schedules[0].schedule_id, "test_schedule")


if __name__ == "__main__":
    unittest.main()
