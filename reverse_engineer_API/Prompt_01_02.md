# KatapultPro Data Export Enhancement Tasks

## 1. SharePoint Aerial Status Report Updates

### New Columns
- Add "Conversation" (Column B) and "Project" (Column C) to the Aerial Status Report
- Source: Job metadata
- Implementation:
  - Update report generation logic to include these fields
  - Modify DataFrame structure in `create_report` function
  - Update SharePoint spreadsheet update logic to handle new columns

### Technical Details
- Extract from `metadata` object in job data
- Fields to extract:
  - `metadata.conversation` for Conversation column
  - `metadata.project` for Project column
- Update Excel formatting to match existing style

## 2. Shapefile and GeoPackage Export Enhancements

### A. Node/Connection ID Tracking
- Add fields:
  - `node_id` for poles/nodes
  - `conn_id` for connections
- Implementation:
  - Update GeoDataFrame structure in `saveMasterGeoPackage`
  - Modify shapefile export logic
- Source: Existing ID fields from node and connection data

### B. Connection Height Field Updates
- Rename "attachment" to "mid_ht"
- Extract mid-height from section photo data
- Implementation steps:
  ```python
  # Similar to POA height extraction:
  photofirst_data = photo_data[section_photo_id].get('photofirst_data', {})
  # Extract height from relevant section
  # Convert to feet/inches format
  ```

### C. Wire Specification Field
- Add "wire_spec" field to connections
- Extract from Clearnetworx fiber optic com data
- Source: Same section photo used for mid_ht
- Implementation:
  - Add logic to extract wire specifications
  - Update connection properties in GeoDataFrame

### D. Project and Conversation Fields
- Add to pole data in shapefile/geopackage
- Source: Job metadata (same as SharePoint report)
- Implementation:
  - Update pole properties in GeoDataFrame
  - Modify field mappings in export functions

## Implementation Notes
1. Update field mappings in relevant functions
2. Maintain existing data validation and error handling
3. Keep current formatting and data type consistency
4. Add appropriate logging for new data extraction
5. Update documentation for new fields

## Code Areas to Modify
1. `create_report` function
2. `saveMasterGeoPackage` function
3. `extractConnections` function
4. `saveToShapefiles` function
5. Field mapping dictionaries
6. SharePoint update logic

## Testing Requirements
1. Verify new columns in SharePoint report
2. Validate mid_ht extraction accuracy
3. Confirm wire_spec data extraction
4. Check node/connection ID presence
5. Verify project and conversation field population