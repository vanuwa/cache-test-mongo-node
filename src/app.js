const express = require('express');
const mongoose = require('mongoose');
const app = express();
const port = 3000;

mongoose.connect('mongodb://localhost:27017/improve_cache');

const placementSchema = new mongoose.Schema({
    placement_id: Number,
    placement_name: String,
    placement_status_id: Number,
    placement_display_status_id: Number,
    placement_type_id: Number,
    placement_type: String,
    placement_type_status_id: Number,
    fold: Number,
    zone_id: Number,
    zone_name: String,
    zone_status_id: Number,
    zone_display_status_id: Number,
    site_id: Number,
    site_name: String,
    site_url: String,
    site_status_id: Number,
    site_display_status_id: Number,
    publisher_id: Number,
    publisher_name: String,
    tag_type_id: Number,
    tag_type: String,
    placement_tag_type: String,
    primary_size_id: Number,
    primary_size: String,
    status_id: Number,
    placement_identifier: String,
    appnexus: Number,
    placement_created_at: Date,
    placement_modified_at: Date
}, { collection: 'raw_placements' });

const Placement = mongoose.model('Placement', placementSchema);

// Create indexes for commonly queried fields
Placement.createIndexes([
    { placement_id: 1 },
    { placement_name: 1 },
    { publisher_id: 1 },
    { site_id: 1 },
    { status_id: 1 },
    { placement_type: 1 },
    { primary_size: 1 }
]);

app.get('/placements', async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 20;
        const skip = (page - 1) * limit;

        const filter = {};

        // Helper function to parse and add filter conditions
        const addFilter = (key, parseFunc = val => val) => {
            if (req.query[key]) {
                const values = req.query[key].split(',').map(parseFunc);
                filter[key] = values.length > 1 ? { $in: values } : values[0];
            }
        };

        // Apply filters
        addFilter('placement_id', parseInt);
        addFilter('placement_name');
        addFilter('placement_status_id', parseInt);
        addFilter('placement_type');
        addFilter('zone_id', parseInt);
        addFilter('site_id', parseInt);
        addFilter('publisher_id', parseInt);
        addFilter('tag_type');
        addFilter('primary_size');
        addFilter('status_id', parseInt);

        // Use lean() for faster query execution
        const placements = await Placement.find(filter)
            .select('placement_id placement_name publisher_id site_id status_id placement_type primary_size')
            .skip(skip)
            .limit(limit)
            .lean();

        // Use countDocuments() with filter for accurate count
        const total = await Placement.countDocuments(filter);

        res.json({
            placements,
            currentPage: page,
            totalPages: Math.ceil(total / limit),
            totalItems: total
        });
    } catch (error) {
        console.error('Error:', error);
        res.status(500).json({ error: 'An error occurred while fetching placements' });
    }
});

app.listen(port, () => {
    console.log(`Server is running on port ${port}`);
});
