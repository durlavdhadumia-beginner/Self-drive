document.addEventListener('DOMContentLoaded', () => {
    const mapContainers = document.querySelectorAll('[data-trip-map]');
    if (!mapContainers.length) {
        return;
    }
    if (typeof L === 'undefined') {
        mapContainers.forEach((container) => {
            container.innerHTML = '<div class="text-muted small text-center py-4">Map preview unavailable (Leaflet not loaded).</div>';
        });
        return;
    }

    const DEFAULT_VIEW = { lat: 20.5937, lng: 78.9629, zoom: 5 };

    mapContainers.forEach((container) => {
        const hasGps = container.getAttribute('data-has-gps') === '1';
        if (!hasGps) {
            return;
        }
        const parseNumber = (value) => {
            const number = parseFloat(value);
            return Number.isFinite(number) ? number : null;
        };
        const carLat = parseNumber(container.getAttribute('data-car-lat'));
        const carLng = parseNumber(container.getAttribute('data-car-lng'));
        const deliveryLat = parseNumber(container.getAttribute('data-delivery-lat'));
        const deliveryLng = parseNumber(container.getAttribute('data-delivery-lng'));

        let destinations = [];
        try {
            destinations = JSON.parse(container.getAttribute('data-destinations') || '[]');
        } catch (error) {
            destinations = [];
        }

        const map = L.map(container, { zoomControl: false, attributionControl: false });
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            maxZoom: 18,
            attribution: '&copy; <a href="https://www.openstreetmap.org">OpenStreetMap</a> contributors',
        }).addTo(map);

        const bounds = [];
        const routePoints = [];
        const addMarker = (lat, lng, options = {}) => {
            if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
                return;
            }
            L.marker([lat, lng], options).addTo(map);
            bounds.push([lat, lng]);
            routePoints.push([lat, lng]);
        };

        if (Number.isFinite(carLat) && Number.isFinite(carLng)) {
            addMarker(carLat, carLng, { title: 'Vehicle location' });
        }

        destinations.forEach((destination, index) => {
            const destLat = parseNumber(destination.latitude);
            const destLng = parseNumber(destination.longitude);
            if (!Number.isFinite(destLat) || !Number.isFinite(destLng)) {
                return;
            }
            addMarker(destLat, destLng, { title: destination.name || `Destination ${index + 1}` });
        });

        if (Number.isFinite(deliveryLat) && Number.isFinite(deliveryLng)) {
            addMarker(deliveryLat, deliveryLng, { title: 'Delivery location' });
        }

        if (routePoints.length >= 2) {
            L.polyline(routePoints, {
                color: '#5b21b6',
                weight: 3,
                opacity: 0.75,
                dashArray: '6,4',
            }).addTo(map);
        }

        if (bounds.length) {
            map.fitBounds(L.latLngBounds(bounds).pad(0.25));
        } else if (Number.isFinite(carLat) && Number.isFinite(carLng)) {
            map.setView([carLat, carLng], 12);
        } else if (Number.isFinite(deliveryLat) && Number.isFinite(deliveryLng)) {
            map.setView([deliveryLat, deliveryLng], 12);
        } else {
            map.setView([DEFAULT_VIEW.lat, DEFAULT_VIEW.lng], DEFAULT_VIEW.zoom);
        }

        setTimeout(() => map.invalidateSize(), 150);
    });
});
