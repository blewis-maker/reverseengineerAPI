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
            
            # Create sheets
            weekly_metrics = self.wb.active
            weekly_metrics.title = "Weekly Status"
            burndown = self.wb.create_sheet("Burndown")
            schedule = self.wb.create_sheet("Schedule")
            charts = self.wb.create_sheet("Charts")
            
            logger.info("Created worksheet structure")
            
            # Generate each section
            logger.info("Generating report sections...")
            self._generate_header(weekly_metrics)
            self._generate_utility_progress(weekly_metrics)
            self._generate_osp_productivity(weekly_metrics)
            self._generate_status_tracking(weekly_metrics)
            self._generate_burndown_metrics(burndown)
            self._generate_schedule_metrics(schedule)
            self._generate_charts(charts)
            
            # Save the workbook
            self.wb.save(output_path)
            logger.info("Weekly report generated successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error generating report: {str(e)}")
            return False
            
    def _generate_header(self, ws):
        """Generate the report header."""
        ws['A1'] = "OSP Production Master - Weekly Status Report"
        ws['A2'] = f"Generated: {self.report_date.strftime('%Y-%m-%d %H:%M:%S')}"
        start_date = self.report_date - timedelta(days=7)
        ws['A3'] = f"Date Range: {start_date.strftime('%Y-%m-%d')} to {self.report_date.strftime('%Y-%m-%d')}"
        
        # Style header
        for cell in ['A1', 'A2', 'A3']:
            ws[cell].font = Font(bold=True)
            
    def _generate_status_tracking(self, ws):
        """Generate the Status Tracking section."""
        logger.info("Generating Status Tracking section")
        current_row = ws.max_row + 2
        
        # Section header
        ws.cell(row=current_row, column=1, value="Status Tracking").font = Font(bold=True)
        current_row += 1
        
        # Headers
        headers = ['Status', 'Job ID', 'Pole Count', 'Utility', 'Original Status Date']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=current_row, column=col)
            cell.value = header
            cell.font = Font(bold=True)
        current_row += 1
        
        # Data rows for each status category
        status_categories = {
            'EMR Status': self.metrics.status_changes['emr'],
            'Approved for Construction': self.metrics.status_changes['approved'],
            'Field Collection': self.metrics.status_changes['field_collection'],
            'Annotation': self.metrics.status_changes['annotation'],
            'Sent to PE': self.metrics.status_changes['sent_to_pe'],
            'Delivery': self.metrics.status_changes['delivery']
        }
        
        total_changes = 0
        for status, changes in status_categories.items():
            logger.info(f"Processing {len(changes)} changes for status: {status}")
            for change in changes:
                ws.cell(row=current_row, column=1).value = status
                ws.cell(row=current_row, column=2).value = change['job_id']
                ws.cell(row=current_row, column=3).value = change['pole_count']
                ws.cell(row=current_row, column=4).value = change['utility']
                ws.cell(row=current_row, column=5).value = change['date']
                current_row += 1
                total_changes += 1
        
        logger.info(f"Added {total_changes} status changes to report")
                
    def _generate_burndown_metrics(self, ws):
        """Generate the Burndown metrics sheet."""
        logger.info("Generating Burndown metrics")
        
        # Section header
        ws['A1'] = "Burndown Analysis"
        ws['A1'].font = Font(bold=True)
        
        # Utility section
        ws['A3'] = "Utility Burndown"
        ws['A3'].font = Font(bold=True)
        
        headers = ['Utility', 'Total Poles', 'Completed Poles', 'Run Rate', 'Est. Completion']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=4, column=col)
            cell.value = header
            cell.font = Font(bold=True)
            
        row = 5
        utility_count = 0
        for utility, metrics in self.metrics.burndown['by_utility'].items():
            ws.cell(row=row, column=1).value = utility
            ws.cell(row=row, column=2).value = metrics['total_poles']
            ws.cell(row=row, column=3).value = metrics['completed_poles']
            ws.cell(row=row, column=4).value = f"{metrics['run_rate']:.2f}"
            ws.cell(row=row, column=5).value = metrics['estimated_completion']
            row += 1
            utility_count += 1
            
        logger.info(f"Added burndown metrics for {utility_count} utilities")
            
        # Backlog section
        ws.cell(row=row+2, column=1, value="Backlog Analysis").font = Font(bold=True)
        row += 3
        
        backlog_headers = ['Category', 'Total Poles', 'Jobs', 'Utilities']
        for col, header in enumerate(backlog_headers, 1):
            cell = ws.cell(row=row, column=col)
            cell.value = header
            cell.font = Font(bold=True)
        row += 1
        
        for category in ['field', 'back_office']:
            ws.cell(row=row, column=1).value = category.replace('_', ' ').title()
            ws.cell(row=row, column=2).value = self.metrics.backlog[category]['total_poles']
            ws.cell(row=row, column=3).value = len(self.metrics.backlog[category]['jobs'])
            ws.cell(row=row, column=4).value = len(self.metrics.backlog[category]['utilities'])
            logger.info(f"{category} backlog: {self.metrics.backlog[category]['total_poles']} poles, {len(self.metrics.backlog[category]['jobs'])} jobs")
            row += 1
            
    def _generate_schedule_metrics(self, ws):
        """Generate the Schedule metrics sheet."""
        logger.info("Generating Schedule metrics")
        
        # Section header
        ws['A1'] = "Project Schedule"
        ws['A1'].font = Font(bold=True)
        
        # Headers
        headers = ['Project', 'Total Poles', 'Completed', 'Progress', 'Field Users', 'Back Office Users', 'Est. Completion']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=3, column=col)
            cell.value = header
            cell.font = Font(bold=True)
            
        # Data rows
        row = 4
        project_count = 0
        for project in self.metrics.schedule['projects']:
            ws.cell(row=row, column=1).value = project['project_id']
            ws.cell(row=row, column=2).value = project['total_poles']
            ws.cell(row=row, column=3).value = project['completed_poles']
            progress = (project['completed_poles'] / project['total_poles'] * 100) if project['total_poles'] > 0 else 0
            ws.cell(row=row, column=4).value = f"{progress:.1f}%"
            ws.cell(row=row, column=5).value = project['field_users']
            ws.cell(row=row, column=6).value = project['back_office_users']
            ws.cell(row=row, column=7).value = project.get('end_date', 'TBD')
            
            logger.info(f"Project {project['project_id']}: {progress:.1f}% complete, {project['total_poles']} total poles")
            row += 1
            project_count += 1
            
        logger.info(f"Added schedule metrics for {project_count} projects")

    def _generate_utility_progress(self, ws):
        """Generate the Utility Progress section."""
        logger.info("Generating Utility Progress section")
        
        # Section header
        ws['A5'] = "Utility Progress"
        ws['A5'].font = Font(bold=True)
        
        # Column headers
        headers = ['Utility', 'Current Rate', 'Previous Rate', 'Change', 'Total Poles', 'Completed Poles']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=6, column=col)
            cell.value = header
            cell.font = Font(bold=True)
            
        # Data rows
        row = 7
        logger.info("Writing utility metrics:")
        for utility, metrics in self.metrics.burndown['by_utility'].items():
            # Skip utilities with no poles
            if metrics['total_poles'] == 0:
                logger.debug(f"Skipping utility {utility} with no poles")
                continue
                
            current_rate = metrics['run_rate']
            previous_rate = 0  # TODO: Get from historical data
            change = current_rate - previous_rate
            
            logger.info(f"  {utility}:")
            logger.info(f"    Current Rate: {current_rate:.1f}")
            logger.info(f"    Total Poles: {metrics['total_poles']}")
            logger.info(f"    Completed Poles: {metrics['completed_poles']}")
            
            ws.cell(row=row, column=1).value = utility
            ws.cell(row=row, column=2).value = f"{current_rate:.1f}"
            ws.cell(row=row, column=3).value = f"{previous_rate:.1f}"
            ws.cell(row=row, column=4).value = f"{change:+.1f}"
            ws.cell(row=row, column=5).value = metrics['total_poles']
            ws.cell(row=row, column=6).value = metrics['completed_poles']
            
            row += 1
            
    def _generate_osp_productivity(self, ws):
        """Generate the OSP Productivity section."""
        # Calculate start row (2 rows after utility section)
        start_row = 7 + len(self.metrics.burndown['by_utility']) + 2
        
        # Section header
        ws.cell(row=start_row, column=1).value = "OSP Productivity"
        ws.cell(row=start_row, column=1).font = Font(bold=True)
        
        # Column headers
        headers = ['Worker', 'Jobs Processed', 'Poles Completed', 'Avg Poles/Job']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=start_row + 1, column=col)
            cell.value = header
            cell.font = Font(bold=True)
            
        # Data rows
        row = start_row + 2
        for user_data in self.metrics.get_weekly_status()['user_production']['annotation']:
            jobs_processed = len(set(user_data['jobs']))
            poles_completed = user_data['completed_poles']
            avg_poles = poles_completed / jobs_processed if jobs_processed > 0 else 0
            
            ws.cell(row=row, column=1).value = user_data['user']
            ws.cell(row=row, column=2).value = jobs_processed
            ws.cell(row=row, column=3).value = poles_completed
            ws.cell(row=row, column=4).value = f"{avg_poles:.1f}"
            
            row += 1
            
    def _generate_charts(self, ws):
        """Generate the charts sheet."""
        # Utility Completion Rates Chart
        utility_chart = BarChart()
        utility_chart.title = "Utility Completion Rates"
        utility_chart.style = 10
        utility_chart.x_axis.title = "Utility"
        utility_chart.y_axis.title = "Rate"
        
        # Get data ranges from Weekly Metrics sheet
        data = Reference(self.wb['Weekly Status'], 
                        min_col=2, max_col=3,  # Current and Previous Rate columns
                        min_row=6,  # Header row
                        max_row=6 + len(self.metrics.burndown['by_utility']))
        cats = Reference(self.wb['Weekly Status'],
                        min_col=1, max_col=1,  # Utility column
                        min_row=7,  # Data start row
                        max_row=6 + len(self.metrics.burndown['by_utility']))
        
        utility_chart.add_data(data, titles_from_data=True)
        utility_chart.set_categories(cats)
        
        # OSP Productivity Chart
        osp_chart = BarChart()
        osp_chart.title = "OSP Productivity"
        osp_chart.style = 10
        osp_chart.x_axis.title = "Worker"
        osp_chart.y_axis.title = "Count"
        
        # Calculate OSP data range
        osp_start_row = 7 + len(self.metrics.burndown['by_utility']) + 3  # Header row for OSP section
        osp_end_row = osp_start_row + len(self.metrics.get_weekly_status()['user_production']['annotation'])
        
        data = Reference(self.wb['Weekly Status'],
                        min_col=2, max_col=3,  # Jobs and Poles columns
                        min_row=osp_start_row,
                        max_row=osp_end_row)
        cats = Reference(self.wb['Weekly Status'],
                        min_col=1, max_col=1,  # Worker column
                        min_row=osp_start_row + 1,
                        max_row=osp_end_row)
        
        osp_chart.add_data(data, titles_from_data=True)
        osp_chart.set_categories(cats)
        
        # Position charts side by side
        ws.add_chart(utility_chart, "B2")
        ws.add_chart(osp_chart, "J2")
        
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