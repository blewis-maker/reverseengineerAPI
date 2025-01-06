import pandas as pd
import os
from datetime import datetime, timedelta
from openpyxl import Workbook
from openpyxl.chart import BarChart, Reference
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
import logging

# Configure logger
logger = logging.getLogger(__name__)

class WeeklyReportGenerator:
    def __init__(self, metrics, report_date=None):
        self.metrics = metrics
        self.current_row = 1  # Initialize current_row
        # Ensure report_date is a datetime object
        if isinstance(report_date, str):
            try:
                self.report_date = datetime.strptime(report_date, '%Y-%m-%d')
            except ValueError:
                self.report_date = datetime.now()
        elif isinstance(report_date, datetime):
            self.report_date = report_date
        else:
            self.report_date = datetime.now()
            
        # Adjust report_date to Sunday 8:00 AM if not already
        days_until_sunday = (6 - self.report_date.weekday()) % 7
        self.report_date = self.report_date + timedelta(days=days_until_sunday)
        self.report_date = self.report_date.replace(hour=8, minute=0, second=0, microsecond=0)
        
        logger.info(f"Initializing Weekly Report Generator for date: {self.report_date}")
        self.wb = Workbook()
        
    def generate_report(self, output_path):
        """Generate the weekly report Excel file."""
        try:
            logger.info(f"Starting weekly report generation at {output_path}")
            logger.info(f"Report period: {(self.report_date - timedelta(days=7)).strftime('%Y-%m-%d')} to {self.report_date.strftime('%Y-%m-%d')}")
            
            # Create sheets
            weekly_metrics = self.wb.active
            weekly_metrics.title = "Weekly Status"
            burndown = self.wb.create_sheet("Burndown")
            schedule = self.wb.create_sheet("Schedule")
            
            logger.info("Created worksheet structure")
            
            # Get weekly status once
            weekly_status = self.metrics.get_weekly_status()
            
            # Generate each section
            logger.info("Generating report sections...")
            self._generate_header(weekly_metrics)
            
            # Generate utility progress
            logger.info("Generating Utility Progress section")
            self._generate_utility_progress(weekly_metrics, weekly_status)
            
            # Generate OSP productivity
            logger.info("Generating OSP Productivity section")
            self._generate_osp_productivity(weekly_metrics, weekly_status)
            
            # Generate status tracking
            logger.info("Generating Status Tracking section")
            self._generate_status_tracking(weekly_metrics, weekly_status)
            
            # Generate burndown metrics
            logger.info("Generating Burndown Metrics section")
            self._generate_burndown_metrics(burndown, weekly_status)
            
            # Generate schedule metrics
            logger.info("Generating Schedule Metrics section")
            self._generate_schedule_metrics(schedule, weekly_status)
            
            # Save the workbook
            self.wb.save(output_path)
            logger.info(f"Report saved to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error generating report: {str(e)}")
            logger.error("Failed to generate report")
            return False
            
    def _generate_header(self, ws):
        """Generate the report header."""
        # Title
        ws['A1'] = "OSP Production Master - Weekly Status Report"
        ws['A1'].font = Font(bold=True)
        
        # Date range in merged cells
        start_date = self.report_date - timedelta(days=7)
        date_range = f"Date Range: {start_date.strftime('%Y-%m-%d')} to {self.report_date.strftime('%Y-%m-%d')}"
        ws['B1'] = date_range
        ws.merge_cells('B1:D1')
        ws['B1'].font = Font(bold=True)
        ws['B1'].alignment = Alignment(horizontal='center')
        
        # Add spacing for the next section
        self.current_row = 3
        
    def _generate_status_tracking(self, worksheet, weekly_status):
        """Generate status tracking section of the report."""
        logging.info("Generating status tracking section")
        
        # Section header
        worksheet.cell(row=self.current_row, column=1).value = "Status Tracking"
        worksheet.cell(row=self.current_row, column=1).font = Font(bold=True)
        self.current_row += 1
        
        # Set up headers
        headers = ['Status', 'Total Jobs', 'Total Poles', 'Change from Last Week']
        for col, header in enumerate(headers):
            cell = worksheet.cell(row=self.current_row, column=col + 1)
            cell.value = header
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color='CCCCCC', end_color='CCCCCC', fill_type='solid')
            
        self.current_row += 1
        
        # Add status changes
        status_changes = weekly_status.get('status_changes', {})
        for status, data in status_changes.items():
            row = [
                status,
                data.get('job_count', 0),
                data.get('pole_count', 0),
                f"+{data.get('change_from_last_week', 0)}" if data.get('change_from_last_week', 0) > 0 else str(data.get('change_from_last_week', 0))
            ]
            for col, value in enumerate(row):
                cell = worksheet.cell(row=self.current_row, column=col + 1)
                cell.value = value
            self.current_row += 1
                
        logging.info(f"Added status tracking data")
        
        # Add spacing
        self.current_row += 2

    def _generate_burndown_metrics(self, worksheet, weekly_status):
        """Generate burndown metrics section of the report."""
        logging.info("Generating burndown metrics section")
        
        # Set up headers for utility burndown
        headers = ['Utility', 'Total Poles', 'Completed Poles', 'Run Rate', 'Est. Completion']
        for col, header in enumerate(headers):
            cell = worksheet.cell(row=self.current_row, column=col + 1)
            cell.value = header
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color='CCCCCC', end_color='CCCCCC', fill_type='solid')
            
        self.current_row += 1
        
        # Add utility burndown metrics
        burndown = weekly_status['burndown']
        utility_start_row = self.current_row
        for utility, metrics in burndown['by_utility'].items():
            if utility == 'Unknown':
                continue
            row = [
                utility,
                metrics['total_poles'],
                metrics['completed_poles'],
                f"{metrics['run_rate']:.1f} poles/week",
                metrics['estimated_completion'].strftime('%Y-%m-%d') if isinstance(metrics['estimated_completion'], datetime) else metrics['estimated_completion'] or 'TBD'
            ]
            for col, value in enumerate(row):
                cell = worksheet.cell(row=self.current_row, column=col + 1)
                cell.value = value
            self.current_row += 1
        utility_end_row = self.current_row - 1
        
        # Add spacing
        self.current_row += 2
        
        # Add project burndown metrics header
        headers = ['Project', 'Total Poles', 'Completed Poles', 'Run Rate', 'Est. Completion']
        for col, header in enumerate(headers):
            cell = worksheet.cell(row=self.current_row, column=col + 1)
            cell.value = header
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color='CCCCCC', end_color='CCCCCC', fill_type='solid')
            
        self.current_row += 1
        project_start_row = self.current_row
        
        # Add project burndown metrics
        for project, metrics in burndown['by_project'].items():
            if project == 'Unknown':
                continue
            row = [
                project,
                metrics['total_poles'],
                metrics['completed_poles'],
                f"{metrics['run_rate']:.1f} poles/week",
                metrics['estimated_completion'].strftime('%Y-%m-%d') if isinstance(metrics['estimated_completion'], datetime) else metrics['estimated_completion'] or 'TBD'
            ]
            for col, value in enumerate(row):
                cell = worksheet.cell(row=self.current_row, column=col + 1)
                cell.value = value
            self.current_row += 1
        project_end_row = self.current_row - 1
        
        # Add spacing
        self.current_row += 2
        
        # Add backlog analysis header
        worksheet.cell(row=self.current_row, column=1).value = "Backlog Analysis"
        worksheet.cell(row=self.current_row, column=1).font = Font(bold=True)
        self.current_row += 1
        
        # Set up headers for backlog
        headers = ['Category', 'Total Poles', 'Jobs', 'Utilities']
        for col, header in enumerate(headers):
            cell = worksheet.cell(row=self.current_row, column=col + 1)
            cell.value = header
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color='CCCCCC', end_color='CCCCCC', fill_type='solid')
            
        self.current_row += 1
        
        # Add backlog metrics
        backlog = burndown['backlog']
        categories = {
            'field': 'Field Collection',
            'back_office': 'Back Office',
            'approve_construction': 'Approve for Construction'
        }
        
        for category, display_name in categories.items():
            if category in backlog:
                metrics = backlog[category]
                utilities_list = sorted(list(metrics['utilities']))  # Convert set to sorted list
                row = [
                    display_name,
                    metrics['total_poles'],
                    len(metrics['jobs']),
                    ', '.join(utilities_list) if utilities_list else 'None'  # Show actual utility names
                ]
                for col, value in enumerate(row):
                    cell = worksheet.cell(row=self.current_row, column=col + 1)
                    cell.value = value
                    # Adjust column width for utilities
                    if col == 3:  # Utilities column
                        worksheet.column_dimensions[get_column_letter(col + 1)].width = max(15, len(str(value)))
                self.current_row += 1
        
        # Generate burndown charts
        # Utility Burndown Chart
        utility_chart = BarChart()
        utility_chart.title = "Utility Burndown"
        utility_chart.style = 10
        utility_chart.x_axis.title = "Utility"
        utility_chart.y_axis.title = "Poles"
        
        data = Reference(worksheet, min_col=2, max_col=3,
                        min_row=utility_start_row - 1, max_row=utility_end_row)
        cats = Reference(worksheet, min_col=1, max_col=1,
                        min_row=utility_start_row, max_row=utility_end_row)
        
        utility_chart.add_data(data, titles_from_data=True)
        utility_chart.set_categories(cats)
        
        # Project Burndown Chart
        project_chart = BarChart()
        project_chart.title = "Project Burndown"
        project_chart.style = 10
        project_chart.x_axis.title = "Project"
        project_chart.y_axis.title = "Poles"
        
        data = Reference(worksheet, min_col=2, max_col=3,
                        min_row=project_start_row - 1, max_row=project_end_row)
        cats = Reference(worksheet, min_col=1, max_col=1,
                        min_row=project_start_row, max_row=project_end_row)
        
        project_chart.add_data(data, titles_from_data=True)
        project_chart.set_categories(cats)
        
        # Add charts to worksheet
        worksheet.add_chart(utility_chart, "G2")
        worksheet.add_chart(project_chart, "G20")
        
        logging.info("Added burndown metrics and charts")
        
        # Add spacing
        self.current_row += 2

    def _generate_schedule_metrics(self, worksheet, weekly_status):
        """Generate schedule metrics section of the report."""
        logging.info("Generating schedule metrics section")
        
        # Set up headers
        headers = ['Project', 'Total Poles', 'Completed Poles', 'Progress', 'Field Users', 'Back Office Users', 'End Date']
        for col, header in enumerate(headers):
            cell = worksheet.cell(row=self.current_row, column=col + 1)
            cell.value = header
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color='CCCCCC', end_color='CCCCCC', fill_type='solid')
            
        self.current_row += 1
        
        # Add project metrics
        schedule = weekly_status['schedule']
        for project in schedule['projects']:
            progress = (project['completed_poles'] / project['total_poles'] * 100) if project['total_poles'] > 0 else 0
            row = [
                project['project_id'],
                project['total_poles'],
                project['completed_poles'],
                f"{progress:.1f}%",
                len(project['field_users']),
                len(project['back_office_users']),
                project.get('end_date', 'TBD')
            ]
            for col, value in enumerate(row):
                cell = worksheet.cell(row=self.current_row, column=col + 1)
                cell.value = value
            self.current_row += 1
            
        logging.info("Added schedule metrics")
        
        # Add spacing
        self.current_row += 2

    def _generate_utility_progress(self, worksheet, weekly_status):
        """Generate utility progress section."""
        # Section header
        cell = worksheet.cell(row=4, column=1)
        cell.value = "Utility Progress"
        cell.font = Font(bold=True)
        
        # Write headers
        headers = ['Utility', 'Total Poles', 'Field Completed', 'Back Office Completed', 'Remaining Poles', 'Run Rate', 'Est. Completion']
        for col, header in enumerate(headers, 1):
            cell = worksheet.cell(row=5, column=col)
            cell.value = header
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color='CCCCCC', end_color='CCCCCC', fill_type='solid')
            
        row = 6
        for utility, data in weekly_status['utility_metrics'].items():
            worksheet.cell(row=row, column=1).value = utility
            worksheet.cell(row=row, column=2).value = data['total_poles']
            worksheet.cell(row=row, column=3).value = data['field_completed']
            worksheet.cell(row=row, column=4).value = data['back_office_completed']
            remaining = data['total_poles'] - data['field_completed'] - data['back_office_completed']
            worksheet.cell(row=row, column=5).value = remaining
            worksheet.cell(row=row, column=6).value = data['run_rate']
            worksheet.cell(row=row, column=7).value = data['estimated_completion']
            row += 1
            
        self.current_row = row + 2  # Update current row with spacing

    def _generate_osp_productivity(self, worksheet, weekly_status):
        """Generate OSP productivity section."""
        self.current_row = self._write_field_productivity(worksheet, weekly_status['user_production']['field'])
        self.current_row = self._write_back_office_productivity(worksheet, weekly_status['user_production'])

    def _write_field_productivity(self, worksheet, field_users):
        """Write field productivity section."""
        # Section header
        cell = worksheet.cell(row=self.current_row, column=1)
        cell.value = "Field Productivity"
        cell.font = Font(bold=True)
        self.current_row += 1
        
        # Write headers
        headers = ['User', 'Completed Poles', 'Utilities', 'Jobs Completed']
        for col, header in enumerate(headers, 1):
            cell = worksheet.cell(row=self.current_row, column=col)
            cell.value = header
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color='CCCCCC', end_color='CCCCCC', fill_type='solid')
            
        self.current_row += 1
        
        for user_data in field_users:
            worksheet.cell(row=self.current_row, column=1).value = user_data['user']
            worksheet.cell(row=self.current_row, column=2).value = user_data['completed_poles']
            worksheet.cell(row=self.current_row, column=3).value = ', '.join(user_data['utilities'])
            jobs_text = ', '.join([f"{job['job_id']} ({job['pole_count']} poles)" for job in user_data['jobs']])
            worksheet.cell(row=self.current_row, column=4).value = jobs_text
            self.current_row += 1
            
        return self.current_row + 1

    def _write_back_office_productivity(self, worksheet, user_production):
        """Write back office productivity section."""
        # Section header
        cell = worksheet.cell(row=self.current_row, column=1)
        cell.value = "Back Office Productivity"
        cell.font = Font(bold=True)
        self.current_row += 1
        
        # Write headers
        headers = ['Category', 'User', 'Completed Poles', 'Utilities', 'Jobs Completed']
        for col, header in enumerate(headers, 1):
            cell = worksheet.cell(row=self.current_row, column=col)
            cell.value = header
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color='CCCCCC', end_color='CCCCCC', fill_type='solid')
            
        self.current_row += 1
        
        categories = ['annotation', 'sent_to_pe', 'delivery', 'emr', 'approved']
        for category in categories:
            for user_data in user_production[category]:
                worksheet.cell(row=self.current_row, column=1).value = category.replace('_', ' ').title()
                worksheet.cell(row=self.current_row, column=2).value = user_data['user']
                worksheet.cell(row=self.current_row, column=3).value = user_data['completed_poles']
                worksheet.cell(row=self.current_row, column=4).value = ', '.join(user_data['utilities'])
                jobs_text = ', '.join([f"{job['job_id']} ({job['pole_count']} poles)" for job in user_data['jobs']])
                worksheet.cell(row=self.current_row, column=5).value = jobs_text
                self.current_row += 1
                
        return self.current_row + 1

    def _format_worksheet(self, ws):
        """Apply consistent formatting to the worksheet."""
        # Adjust column widths
        for col in ws.columns:
            max_length = 0
            column = col[0].column_letter
            
            for cell in col:
                if cell.value:
                    max_length = max(max_length, len(str(cell.value)))
            
            adjusted_width = (max_length + 2)
            ws.column_dimensions[column].width = adjusted_width
        
        # Center align all cells
        for row in ws.rows:
            for cell in row:
                cell.alignment = Alignment(horizontal='center') 