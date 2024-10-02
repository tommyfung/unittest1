import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
from flask import Flask
import main

class TestMain(unittest.TestCase):

    def setUp(self):
        self.app = main.app
        self.app.testing = True
        self.client = self.app.test_client()

    @patch('main.send_files')
    @patch('main.rename_files')
    @patch('main.default')
    def test_external_file_transfer_send(self, mock_default, mock_rename_files, mock_send_files):
        # Mock GCP credentials
        mock_default.return_value = (MagicMock(), 'project-id')
        mock_send_files.return_value = 'Files sent successfully'
        
        # Mock request data
        with self.app.test_request_context('/external_file_transfer', json={'action': 'send', 'date': '20230925'}):
            response = self.client.post('/external_file_transfer', json={'action': 'send', 'date': '20230925'})
            
            # Assertions
            self.assertEqual(response.status_code, 200)
            self.assertIn('Files sent successfully', response.json['result'])
            mock_rename_files.assert_called_once()
            mock_send_files.assert_called_once()

    @patch('main.get_files')
    @patch('main.default')
    def test_external_file_transfer_get(self, mock_default, mock_get_files):
        # Mock GCP credentials
        mock_default.return_value = (MagicMock(), 'project-id')
        mock_get_files.return_value = 'Files retrieved successfully'
        
        # Mock request data
        with self.app.test_request_context('/external_file_transfer', json={'action': 'get', 'date': '20230925'}):
            response = self.client.post('/external_file_transfer', json={'action': 'get', 'date': '20230925'})
            
            # Assertions
            self.assertEqual(response.status_code, 200)
            self.assertIn('Files retrieved successfully', response.json['result'])
            mock_get_files.assert_called_once()

    @patch('main.zip_files')
    @patch('main.archive_files')
    @patch('main.default')
    def test_external_file_transfer_zip(self, mock_default, mock_archive_files, mock_zip_files):
        # Mock GCP credentials
        mock_default.return_value = (MagicMock(), 'project-id')
        mock_zip_files.return_value = 'Files zipped successfully'
        
        # Mock request data
        with self.app.test_request_context('/external_file_transfer', json={'action': 'zip', 'date': '20230925'}):
            response = self.client.post('/external_file_transfer', json={'action': 'zip', 'date': '20230925'})
            
            # Assertions
            self.assertEqual(response.status_code, 200)
            self.assertIn('Files zipped successfully', response.json['result'])
            mock_zip_files.assert_called_once()
            mock_archive_files.assert_called_once()

    @patch('main.get_last_working_day')
    def test_get_last_working_day(self, mock_get_last_working_day):
        # Mock the return value
        mock_get_last_working_day.return_value = datetime(2023, 9, 29)
        
        # Call the function
        result = main.get_last_working_day(2023, 9)
        
        # Assertions
        self.assertEqual(result, datetime(2023, 9, 29))
        mock_get_last_working_day.assert_called_once_with(2023, 9)

    def test_last_working_day_of_last_month(self):
        # Test the function with a known date
        date = datetime(2023, 10, 1)
        result = main.last_working_day_of_last_month(date)
        
        # Assertions
        self.assertEqual(result, datetime(2023, 9, 29))

    def test_invalid_action(self):
        # Test the function with an invalid action
        with self.app.test_request_context('/external_file_transfer', json={'action': 'invalid', 'date': '20230925'}):
            response = self.client.post('/external_file_transfer', json={'action': 'invalid', 'date': '20230925'})
            
            # Assertions
            self.assertEqual(response.status_code, 400)
            self.assertIn('Invalid action', response.json['error'])

if __name__ == '__main__':
    unittest.main()
