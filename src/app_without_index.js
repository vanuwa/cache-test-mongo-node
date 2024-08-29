const express = require('express');
const mongoose = require('mongoose');
const app = express();
const port = 3000;

// Connect to MongoDB with specific database name
mongoose.connect('mongodb://localhost:27017/improve_cache')
    .then(() => console.log('Connected to MongoDB (improve_cache)'))
    .catch(err => console.error('Error connecting to MongoDB:', err));

// Define the Placement schema
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
}, { collection: 'raw_placements' });  // Specify the collection name

const Placement = mongoose.model('Placement', placementSchema);

// Helper function to parse multiple values from query string
const parseMultipleValues = (value) => {
    return value ? value.split(',').map(v => v.trim()) : [];
};

// API endpoint for getting placements with pagination and enhanced filtering
app.get('/placements', async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 20;
        const skip = (page - 1) * limit;

        // Build the filter object based on query parameters
        const filter = {};

        if (req.query.placement_id) {
            const ids = parseMultipleValues(req.query.placement_id);
            filter.placement_id = { $in: ids.map(id => parseInt(id)) };
        }
        if (req.query.placement_name) {
            const names = parseMultipleValues(req.query.placement_name);
            filter.placement_name = { $in: names.map(name => new RegExp(name, 'i')) };
        }
        if (req.query.publisher_id) {
            const ids = parseMultipleValues(req.query.publisher_id);
            filter.publisher_id = { $in: ids.map(id => parseInt(id)) };
        }
        if (req.query.site_id) {
            const ids = parseMultipleValues(req.query.site_id);
            filter.site_id = { $in: ids.map(id => parseInt(id)) };
        }
        if (req.query.status_id) {
            const ids = parseMultipleValues(req.query.status_id);
            filter.status_id = { $in: ids.map(id => parseInt(id)) };
        }
        if (req.query.placement_type) {
            const types = parseMultipleValues(req.query.placement_type);
            filter.placement_type = { $in: types.map(type => new RegExp(type, 'i')) };
        }
        if (req.query.primary_size) {
            const sizes = parseMultipleValues(req.query.primary_size);
            filter.primary_size = { $in: sizes.map(size => new RegExp(size, 'i')) };
        }

        // Execute the query with pagination
        const placements = await Placement.find(filter)
            .skip(skip)
            .limit(limit)
            .lean();

        // Get total count for pagination metadata
        const totalCount = await Placement.countDocuments(filter);

        res.json({
            data: placements,
            metadata: {
                total: totalCount,
                page: page,
                limit: limit,
                totalPages: Math.ceil(totalCount / limit),
            },
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.listen(port, () => {
    console.log(`Placement API listening at http://localhost:${port}`);
});
