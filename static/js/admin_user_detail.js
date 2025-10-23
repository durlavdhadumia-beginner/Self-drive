document.addEventListener('DOMContentLoaded', () => {
    const MAX_GALLERY_IMAGES = 8;
    const DEFAULT_MAP_VIEW = { lat: 20.5937, lng: 78.9629, zoom: 5 };
    const CITY_ENTRIES = Array.isArray(window.ADMIN_CITY_ENTRIES) ? window.ADMIN_CITY_ENTRIES : [];

    const formatCurrency = (value) => {
        const numeric = Number(value);
        if (!Number.isFinite(numeric)) {
            return '0';
        }
        return Math.round(numeric).toLocaleString('en-IN');
    };

    const buildCityLabel = (entry) => {
        if (!entry) {
            return '';
        }
        const name = (entry.name || '').trim();
        const state = (entry.state || '').trim();
        return state ? `${name}, ${state}` : name;
    };

    const findCityEntry = (label) => {
        if (!label) {
            return null;
        }
        const normalised = label.trim().toLowerCase();
        return CITY_ENTRIES.find((entry) => buildCityLabel(entry).toLowerCase() === normalised) || null;
    };

    const buildLocationIcon = (className = 'map-pin-car', label = '') => {
        if (typeof L === 'undefined') {
            return null;
        }
        return L.divIcon({
            className,
            html: `<span class="map-pin ${label ? 'map-pin-labeled' : ''}">${label}</span>`,
            iconSize: [30, 30],
            iconAnchor: [15, 28],
            popupAnchor: [0, -20],
        });
    };

    const carIcon = buildLocationIcon('map-pin-car', 'C');
    const deliveryIcon = buildLocationIcon('map-pin-delivery', 'D');

    const createPhotoManager = ({ containerId, addButtonId, inputName, initialMax = MAX_GALLERY_IMAGES }) => {
        const container = document.getElementById(containerId);
        const addButton = document.getElementById(addButtonId);
        if (!container || !addButton) {
            return null;
        }
        let maxInputs = typeof initialMax === 'number' ? initialMax : MAX_GALLERY_IMAGES;

        const rows = () => Array.from(container.querySelectorAll('[data-role="photo-input-row"]'));

        const updateAddButtonState = () => {
            addButton.disabled = maxInputs > 0 && rows().length >= maxInputs;
        };

        const buildRow = () => {
            const row = document.createElement('div');
            row.className = 'input-group admin-photo-input-row';
            row.dataset.role = 'photo-input-row';

            const input = document.createElement('input');
            input.type = 'file';
            input.name = inputName;
            input.accept = 'image/*';
            input.className = 'form-control';
            input.addEventListener('change', ensureSpare);

            const removeBtn = document.createElement('button');
            removeBtn.type = 'button';
            removeBtn.className = 'btn btn-outline-danger';
            removeBtn.dataset.role = 'remove-photo';
            removeBtn.innerHTML = '<i class="bi bi-x-lg"></i>';
            removeBtn.addEventListener('click', () => {
                const currentRows = rows();
                if (currentRows.length <= 1) {
                    input.value = '';
                    updateAddButtonState();
                    return;
                }
                row.remove();
                ensureSpare();
            });

            row.appendChild(input);
            row.appendChild(removeBtn);
            return row;
        };

        const ensureSpare = () => {
            if (maxInputs <= 0) {
                container.innerHTML = '';
                updateAddButtonState();
                return;
            }
            const currentRows = rows();
            const hasEmpty = currentRows.some((row) => {
                const input = row.querySelector('input[type="file"]');
                return input && input.files.length === 0;
            });
            if (!hasEmpty && currentRows.length < maxInputs) {
                container.appendChild(buildRow());
            }
            updateAddButtonState();
        };

        addButton.addEventListener('click', () => {
            if (maxInputs > 0 && rows().length >= maxInputs) {
                updateAddButtonState();
                return;
            }
            const row = buildRow();
            container.appendChild(row);
            const input = row.querySelector('input[type="file"]');
            if (input) {
                input.focus();
            }
            updateAddButtonState();
        });

        const reset = (newMax = initialMax) => {
            maxInputs = typeof newMax === 'number' ? newMax : initialMax;
            container.innerHTML = '';
            if (maxInputs > 0) {
                container.appendChild(buildRow());
            }
            ensureSpare();
        };

        reset(initialMax);

        return {
            reset,
            setMax: (value) => {
                maxInputs = typeof value === 'number' ? value : maxInputs;
                ensureSpare();
            },
            ensureSpare,
        };
    };

    const detailModalEl = document.getElementById('adminCarModal');
    const editModalEl = document.getElementById('adminEditCarModal');
    const bootstrapNamespace = (typeof window !== 'undefined' && window.bootstrap)
        ? window.bootstrap
        : (typeof bootstrap !== 'undefined' ? bootstrap : null);
    const createModalInstance = (element) => {
        if (!element || !bootstrapNamespace || typeof bootstrapNamespace.Modal !== 'function') {
            return null;
        }
        return new bootstrapNamespace.Modal(element);
    };
    const detailModal = createModalInstance(detailModalEl);
    const editModal = createModalInstance(editModalEl);

    if (editModalEl && bootstrapNamespace && typeof bootstrapNamespace.Modal === 'function') {
        editModalEl.addEventListener('shown.bs.modal', () => {
            const map = ensureEditMap();
            if (map) {
                setTimeout(() => map.invalidateSize(), 120);
            }
        });
    }

    const detailMapEl = document.getElementById('admin-detail-map');
    let detailMap = null;
    let detailMarker = null;

    const editMapEl = document.getElementById('admin-edit-location-map');
    let editMap = null;
    let editMarker = null;

    const detailSummary = {
        name: detailModalEl ? detailModalEl.querySelector('[data-admin-detail="name"]') : null,
        subtitle: detailModalEl ? detailModalEl.querySelector('[data-admin-detail="subtitle"]') : null,
        city: detailModalEl ? detailModalEl.querySelector('[data-admin-detail="city"]') : null,
        rates: detailModalEl ? detailModalEl.querySelector('[data-admin-detail="rates"]') : null,
        vehicle: detailModalEl ? detailModalEl.querySelector('[data-admin-detail="vehicle"]') : null,
        fuel: detailModalEl ? detailModalEl.querySelector('[data-admin-detail="fuel"]') : null,
        delivery: detailModalEl ? detailModalEl.querySelector('[data-admin-detail="delivery"]') : null,
        description: detailModalEl ? detailModalEl.querySelector('[data-admin-detail="description"]') : null,
        gallery: detailModalEl ? detailModalEl.querySelector('[data-admin-detail="gallery"]') : null,
        locationSummary: detailModalEl ? detailModalEl.querySelector('[data-admin-detail="location-summary"]') : null,
        editButton: detailModalEl ? detailModalEl.querySelector('[data-admin-detail="open-edit"]') : null,
    };

    const editForm = document.getElementById('admin-edit-car-form');
    const editFeedback = editModalEl ? editModalEl.querySelector('[data-admin-edit-feedback]') : null;
    const submitButton = editModalEl ? editModalEl.querySelector('[data-admin-edit-submit]') : null;
    const editCityInput = document.getElementById('admin-edit-city');
    const editLatInput = document.getElementById('admin-edit-latitude');
    const editLngInput = document.getElementById('admin-edit-longitude');
    const locationSummary = editModalEl ? editModalEl.querySelector('[data-admin-location-summary]') : null;
    const currentImagesContainer = editModalEl ? editModalEl.querySelector('[data-admin-current-images]') : null;
    const photoHelper = editModalEl ? editModalEl.querySelector('[data-admin-photo-helper]') : null;
    const editPhotoManager = createPhotoManager({
        containerId: 'admin-edit-photo-input-list',
        addButtonId: 'admin-add-photo-input',
        inputName: 'new_photos',
        initialMax: MAX_GALLERY_IMAGES,
    });

    let existingImages = [];
    const imagesMarkedForDeletion = new Set();
    let activeCarId = null;

    const showFeedback = (message, type = 'success') => {
        if (!editFeedback) {
            window.alert(message);
            return;
        }
        editFeedback.classList.remove('d-none', 'alert-success', 'alert-danger');
        editFeedback.classList.add(type === 'success' ? 'alert-success' : 'alert-danger');
        editFeedback.textContent = message;
    };

    const resetFeedback = () => {
        if (!editFeedback) {
            return;
        }
        editFeedback.classList.add('d-none');
        editFeedback.textContent = '';
    };

    const buildDeliverySummary = (options = {}) => {
        const entries = Object.entries(options)
            .map(([distance, price]) => [Number(distance), Number(price)])
            .filter(([distance]) => Number.isFinite(distance))
            .sort((a, b) => a[0] - b[0]);
        if (!entries.length) {
            return 'Delivery: Not offered';
        }
        const parts = entries.map(([distance, price]) => `Up to ${distance} km (₹${Math.round(price)})`);
        return `Delivery: ${parts.join(', ')}`;
    };

    const updatePhotoLimitMessage = (allowed) => {
        if (!photoHelper) {
            return;
        }
        if (allowed <= 0) {
            photoHelper.textContent = 'You have reached the 8 image limit. Remove an image to upload more.';
        } else {
            photoHelper.textContent = `Upload up to ${allowed} more image${allowed === 1 ? '' : 's'} (maximum of 8).`;
        }
    };

    const updateNewPhotoLimit = () => {
        const remaining = MAX_GALLERY_IMAGES - (existingImages.length - imagesMarkedForDeletion.size);
        if (editPhotoManager) {
            editPhotoManager.setMax(Math.max(remaining, 0));
            editPhotoManager.ensureSpare();
        }
        updatePhotoLimitMessage(Math.max(remaining, 0));
    };

    const renderExistingImages = () => {
        if (!currentImagesContainer) {
            return;
        }
        currentImagesContainer.innerHTML = '';
        if (!existingImages.length) {
            const emptyState = document.createElement('p');
            emptyState.className = 'text-muted small mb-0';
            emptyState.textContent = 'No gallery photos yet.';
            currentImagesContainer.appendChild(emptyState);
            updateNewPhotoLimit();
            return;
        }
        existingImages.forEach((image) => {
            const wrapper = document.createElement('div');
            wrapper.className = 'd-flex flex-column align-items-center';
            wrapper.style.width = '110px';

            const img = document.createElement('img');
            img.src = image.url || image.filename;
            img.alt = image.filename || 'Vehicle photo';
            img.className = 'rounded mb-2 w-100';
            img.style.height = '74px';
            img.style.objectFit = 'cover';
            if (imagesMarkedForDeletion.has(image.id)) {
                img.classList.add('opacity-50');
            }

            const toggleButton = document.createElement('button');
            toggleButton.type = 'button';
            toggleButton.className = 'btn btn-sm w-100';
            const updateButtonState = () => {
                if (imagesMarkedForDeletion.has(image.id)) {
                    toggleButton.className = 'btn btn-sm w-100 btn-outline-secondary';
                    toggleButton.innerHTML = '<i class="bi bi-arrow-counterclockwise me-1"></i>Undo remove';
                } else {
                    toggleButton.className = 'btn btn-sm w-100 btn-outline-danger';
                    toggleButton.innerHTML = '<i class="bi bi-trash me-1"></i>Remove';
                }
            };
            toggleButton.addEventListener('click', () => {
                if (imagesMarkedForDeletion.has(image.id)) {
                    imagesMarkedForDeletion.delete(image.id);
                    img.classList.remove('opacity-50');
                } else {
                    imagesMarkedForDeletion.add(image.id);
                    img.classList.add('opacity-50');
                }
                updateButtonState();
                updateNewPhotoLimit();
            });
            updateButtonState();

            wrapper.appendChild(img);
            wrapper.appendChild(toggleButton);
            currentImagesContainer.appendChild(wrapper);
        });
        updateNewPhotoLimit();
    };

    const populateDeliveryRows = (options = {}) => {
        if (!editModalEl) {
            return;
        }
        editModalEl.querySelectorAll('[data-admin-delivery-row]').forEach((row) => {
            const distance = Number(row.getAttribute('data-admin-delivery-row') || '0');
            const checkbox = row.querySelector('[data-admin-delivery-checkbox]');
            const priceInput = row.querySelector('[data-admin-delivery-price]');
            if (!checkbox || !priceInput) {
                return;
            }
            const fee = options[String(distance)] ?? options[distance];
            if (fee !== undefined && fee !== null) {
                checkbox.checked = true;
                priceInput.disabled = false;
                priceInput.value = Number(fee);
            } else {
                checkbox.checked = false;
                priceInput.disabled = true;
                priceInput.value = '';
            }
        });
    };

    const wireDeliveryRows = () => {
        if (!editModalEl) {
            return;
        }
        editModalEl.querySelectorAll('[data-admin-delivery-checkbox]').forEach((checkbox) => {
            checkbox.addEventListener('change', () => {
                const row = checkbox.closest('[data-admin-delivery-row]');
                if (!row) {
                    return;
                }
                const priceInput = row.querySelector('[data-admin-delivery-price]');
                if (!priceInput) {
                    return;
                }
                if (checkbox.checked) {
                    priceInput.disabled = false;
                    if (!priceInput.value) {
                        priceInput.focus();
                    }
                } else {
                    priceInput.disabled = true;
                    priceInput.value = '';
                }
            });
        });
    };
    wireDeliveryRows();

    const ensureDetailMap = () => {
        if (!detailMapEl || typeof L === 'undefined') {
            return null;
        }
        if (detailMap) {
            return detailMap;
        }
        detailMap = L.map(detailMapEl, { zoomControl: false, attributionControl: false });
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            maxZoom: 18,
            attribution: '&copy; <a href="https://www.openstreetmap.org">OpenStreetMap</a> contributors',
        }).addTo(detailMap);
        return detailMap;
    };

    const updateDetailMap = (car) => {
        if (!detailSummary.locationSummary) {
            return;
        }
        const map = ensureDetailMap();
        if (!map) {
            detailSummary.locationSummary.textContent = 'Map unavailable.';
            return;
        }
        const lat = Number(car.latitude);
        const lng = Number(car.longitude);
        if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
            detailSummary.locationSummary.textContent = 'Location not set.';
            if (detailMarker) {
                detailMarker.remove();
                detailMarker = null;
            }
            map.setView([DEFAULT_MAP_VIEW.lat, DEFAULT_MAP_VIEW.lng], DEFAULT_MAP_VIEW.zoom);
        } else {
            detailSummary.locationSummary.textContent = `Lat ${lat.toFixed(4)}, Lng ${lng.toFixed(4)}`;
            if (!detailMarker) {
                detailMarker = L.marker([lat, lng], { icon: carIcon || undefined }).addTo(map);
            } else {
                detailMarker.setLatLng([lat, lng]);
            }
            map.setView([lat, lng], 13);
        }
        setTimeout(() => map.invalidateSize(), 150);
    };

    const ensureEditMap = () => {
        if (!editMapEl || typeof L === 'undefined') {
            return null;
        }
        if (editMap) {
            return editMap;
        }
        editMap = L.map(editMapEl, { attributionControl: false });
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            maxZoom: 18,
            attribution: '&copy; <a href="https://www.openstreetmap.org">OpenStreetMap</a> contributors',
        }).addTo(editMap);
        editMap.on('click', (event) => {
            const { lat, lng } = event.latlng;
            setEditLocation(lat, lng);
        });
        return editMap;
    };

    const updateLocationSummary = () => {
        if (!locationSummary || !editLatInput || !editLngInput) {
            return;
        }
        const lat = Number(editLatInput.value);
        const lng = Number(editLngInput.value);
        if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
            locationSummary.textContent = 'Latitude and longitude not set.';
            return;
        }
        locationSummary.textContent = `Current pin: ${lat.toFixed(4)}, ${lng.toFixed(4)}`;
    };

    const setEditLocation = (lat, lng, { pan = true } = {}) => {
        if (editLatInput) {
            editLatInput.value = Number(lat).toFixed(6);
        }
        if (editLngInput) {
            editLngInput.value = Number(lng).toFixed(6);
        }
        const map = ensureEditMap();
        if (map) {
            if (!editMarker) {
                editMarker = L.marker([lat, lng], { draggable: true, icon: carIcon || undefined }).addTo(map);
                editMarker.on('dragend', () => {
                    const position = editMarker.getLatLng();
                    setEditLocation(position.lat, position.lng, { pan: false });
                });
            } else {
                editMarker.setLatLng([lat, lng]);
            }
            if (pan) {
                map.setView([lat, lng], Math.max(map.getZoom(), 13));
            }
            setTimeout(() => map.invalidateSize(), 150);
        }
        updateLocationSummary();
    };

    const updateCardDisplay = (car) => {
        const cardEl = document.querySelector(`[data-admin-car-card="${car.id}"]`);
        if (!cardEl) {
            return;
        }
        if (car.detail_url) {
            cardEl.setAttribute('data-admin-detail-url', car.detail_url);
        }
        if (car.update_url) {
            cardEl.setAttribute('data-admin-update-url', car.update_url);
        }
        const displayName = car.display_name || car.name || `${car.brand || ''} ${car.model || ''}`.trim();
        const subtitle = `${car.brand || 'Unknown'} ${car.model || ''}`.trim();
        const nameEl = cardEl.querySelector('[data-admin-role="name"]');
        if (nameEl) {
            nameEl.textContent = displayName || 'Vehicle';
        }
        const subtitleEl = cardEl.querySelector('[data-admin-role="subtitle"]');
        if (subtitleEl) {
            subtitleEl.textContent = `${subtitle}${car.city ? ` \u2022 ${car.city}` : ' \u2022 City not set'}`;
        }
        const availabilityEl = cardEl.querySelector('[data-admin-role="availability"]');
        if (availabilityEl) {
            const available = Boolean(car.is_available);
            availabilityEl.textContent = available ? 'Available' : 'Unavailable';
            availabilityEl.classList.toggle('bg-success', available);
            availabilityEl.classList.toggle('bg-secondary', !available);
        }
        const licenceEl = cardEl.querySelector('[data-admin-role="licence"]');
        if (licenceEl) {
            licenceEl.textContent = `Licence: ${car.licence_plate || 'Pending'}`;
        }
        const seatsEl = cardEl.querySelector('[data-admin-role="seats"]');
        if (seatsEl) {
            seatsEl.textContent = `Seats: ${car.seats ?? 'N/A'}`;
        }
        const rateHourEl = cardEl.querySelector('[data-admin-role="rate-hour"]');
        if (rateHourEl) {
            rateHourEl.textContent = `Hourly rate: ₹${formatCurrency(car.rate_per_hour)}`;
        }
        const rateDayEl = cardEl.querySelector('[data-admin-role="rate-day"]');
        if (rateDayEl) {
            rateDayEl.textContent = `Daily rate: ₹${formatCurrency(car.daily_rate)}`;
        }
        const fuelEl = cardEl.querySelector('[data-admin-role="fuel"]');
        if (fuelEl) {
            fuelEl.textContent = `Fuel: ${car.fuel_type || 'N/A'}`;
        }
        const transmissionEl = cardEl.querySelector('[data-admin-role="transmission"]');
        if (transmissionEl) {
            transmissionEl.textContent = `Transmission: ${car.transmission || 'N/A'}`;
        }
        const locationLabel = cardEl.querySelector('[data-admin-role="location-label"]');
        if (locationLabel) {
            const parts = [];
            if (car.city) {
                parts.push(car.city);
            }
            if (Number.isFinite(car.latitude) && Number.isFinite(car.longitude)) {
                parts.push(`Lat ${Number(car.latitude).toFixed(4)}, Lng ${Number(car.longitude).toFixed(4)}`);
            }
            locationLabel.textContent = parts.length ? `Location: ${parts.join('  ')}` : 'Location: Not set';
        }
        const deliverySummaryEl = cardEl.querySelector('[data-admin-role="delivery-summary"]');
        if (deliverySummaryEl) {
            deliverySummaryEl.textContent = buildDeliverySummary(car.delivery_options || {});
        }
        const galleryEl = cardEl.querySelector('[data-admin-role="gallery"]');
        if (galleryEl) {
            galleryEl.innerHTML = '';
            const images = Array.isArray(car.images) ? car.images : [];
            if (images.length) {
                images.forEach((image) => {
                    const button = document.createElement('button');
                    button.type = 'button';
                    button.className = 'btn btn-link p-0';
                    button.dataset.adminView = String(car.id);
                    const img = document.createElement('img');
                    img.src = image.url || image.filename;
                    img.alt = displayName || 'Vehicle photo';
                    img.className = 'admin-gallery-thumb';
                    button.appendChild(img);
                    galleryEl.appendChild(button);
                });
            } else {
                const addBtn = document.createElement('button');
                addBtn.type = 'button';
                addBtn.className = 'btn btn-outline-secondary btn-sm';
                addBtn.dataset.adminEdit = String(car.id);
                addBtn.innerHTML = '<i class="bi bi-plus-circle me-1"></i>Add photos';
                galleryEl.appendChild(addBtn);
            }
        }
    };

    const populateDetailModal = (car) => {
        if (!detailSummary.name) {
            return;
        }
        const displayName = car.display_name || car.name || `${car.brand || ''} ${car.model || ''}`.trim() || 'Vehicle';
        detailSummary.name.textContent = displayName;
        detailSummary.subtitle.textContent = `${car.brand || 'Unknown'} ${car.model || ''}`.trim();
        detailSummary.city.textContent = car.city || 'City not set';
        detailSummary.rates.textContent = `₹${formatCurrency(car.rate_per_hour)} / hr \u2022 ₹${formatCurrency(car.daily_rate)} per day`;
        detailSummary.vehicle.textContent = `${car.vehicle_type || 'Vehicle'} \u2022 ${car.size_category || 'Size not set'}`;
        detailSummary.fuel.textContent = `${car.fuel_type || 'Fuel N/A'} \u2022 ${car.transmission || 'Transmission N/A'}`;
        detailSummary.description.textContent = car.description || 'No highlights provided for this listing.';

        if (detailSummary.delivery) {
            detailSummary.delivery.innerHTML = '';
            const entries = Object.entries(car.delivery_options || {})
                .map(([distance, price]) => [Number(distance), Number(price)])
                .filter(([distance]) => Number.isFinite(distance))
                .sort((a, b) => a[0] - b[0]);
            if (!entries.length) {
                detailSummary.delivery.innerHTML = '<li class="list-group-item text-muted small">No delivery charges configured.</li>';
            } else {
                entries.forEach(([distance, price]) => {
                    const item = document.createElement('li');
                    item.className = 'list-group-item d-flex justify-content-between align-items-center small';
                    item.innerHTML = `<span>Up to ${distance} km</span><span class="text-muted">₹${formatCurrency(price)}</span>`;
                    detailSummary.delivery.appendChild(item);
                });
            }
        }

        if (detailSummary.gallery) {
            detailSummary.gallery.innerHTML = '';
            const images = Array.isArray(car.images) ? car.images : [];
            if (!images.length) {
                detailSummary.gallery.innerHTML = '<span class="text-muted small">No images uploaded.</span>';
            } else {
                images.forEach((image) => {
                    const button = document.createElement('button');
                    button.type = 'button';
                    button.className = 'btn btn-link p-0';
                    button.dataset.adminDetailImage = image.url || image.filename;
                    const img = document.createElement('img');
                    img.src = image.url || image.filename;
                    img.alt = displayName;
                    img.className = 'admin-gallery-thumb';
                    button.appendChild(img);
                    detailSummary.gallery.appendChild(button);
                });
            }
        }

        if (detailSummary.editButton) {
            detailSummary.editButton.dataset.adminEdit = String(car.id);
            detailSummary.editButton.disabled = false;
        }
        updateDetailMap(car);
    };

    const populateEditModal = (car) => {
        if (!editForm) {
            return;
        }
        activeCarId = car.id;
        editForm.action = car.update_url || `/owner/cars/${car.id}/update`;
        editForm.querySelectorAll('[data-admin-field]').forEach((field) => {
            const key = field.getAttribute('data-admin-field');
            if (!key) {
                return;
            }
            if (field.type === 'checkbox') {
                field.checked = Boolean(car[key]);
            } else if (field.type === 'number' && field.step && Number(field.step) % 1 !== 0) {
                field.value = car[key] !== undefined && car[key] !== null ? Number(car[key]).toFixed(6) : '';
            } else {
                field.value = car[key] !== undefined && car[key] !== null ? car[key] : '';
            }
        });
        existingImages = Array.isArray(car.images) ? car.images : [];
        imagesMarkedForDeletion.clear();
        renderExistingImages();
        populateDeliveryRows(car.delivery_options || {});
        resetFeedback();
        const lat = Number(car.latitude);
        const lng = Number(car.longitude);
        if (Number.isFinite(lat) && Number.isFinite(lng)) {
            setEditLocation(lat, lng, { pan: false });
        } else if (editCityInput) {
            const entry = findCityEntry(editCityInput.value);
            if (entry && Number.isFinite(entry.latitude) && Number.isFinite(entry.longitude)) {
                setEditLocation(entry.latitude, entry.longitude, { pan: false });
            } else {
                ensureEditMap();
                updateLocationSummary();
            }
        } else {
            ensureEditMap();
            updateLocationSummary();
        }
        if (editPhotoManager) {
            editPhotoManager.reset(MAX_GALLERY_IMAGES);
        }
        updateNewPhotoLimit();
    };

    const resolveCarUrl = (carId, attribute) => {
        const cardEl = document.querySelector(`[data-admin-car-card="${carId}"]`);
        if (!cardEl) {
            return null;
        }
        const value = cardEl.getAttribute(attribute);
        return value && value.trim() ? value : null;
    };

    const fetchCarData = async (carId) => {
        const detailUrl = resolveCarUrl(carId, 'data-admin-detail-url') || `/owner/cars/${carId}/data`;
        const response = await fetch(detailUrl, { headers: { 'X-Requested-With': 'XMLHttpRequest' } });
        if (!response.ok) {
            throw new Error('Unable to load vehicle data.');
        }
        const payload = await response.json();
        if (!payload || !payload.car) {
            throw new Error('Vehicle data was missing in the response.');
        }
        return payload.car;
    };

    const openDetailModal = async (carId) => {
        try {
            const car = await fetchCarData(carId);
            populateDetailModal(car);
            if (detailModal) {
                detailModal.show();
                setTimeout(() => {
                    if (detailMap) {
                        detailMap.invalidateSize();
                    }
                }, 160);
            }
        } catch (error) {
            window.alert(error.message || 'Something went wrong while loading vehicle details.');
        }
    };

    const openEditModal = async (carId) => {
        try {
            const car = await fetchCarData(carId);
            populateEditModal(car);
            if (editModal) {
                editModal.show();
                setTimeout(() => {
                    if (editMap) {
                        editMap.invalidateSize();
                    }
                }, 180);
            }
        } catch (error) {
            window.alert(error.message || 'Something went wrong while loading vehicle details.');
        }
    };

    document.addEventListener('click', (event) => {
        const viewTrigger = event.target.closest('[data-admin-view]');
        if (viewTrigger) {
            const carId = viewTrigger.getAttribute('data-admin-view');
            if (carId) {
                openDetailModal(carId);
            }
            return;
        }
        const editTrigger = event.target.closest('[data-admin-edit]');
        if (editTrigger) {
            const carId = editTrigger.getAttribute('data-admin-edit');
            if (carId) {
                openEditModal(carId);
            }
            return;
        }
        const detailImageTrigger = event.target.closest('[data-admin-detail-image]');
        if (detailImageTrigger) {
            const href = detailImageTrigger.getAttribute('data-admin-detail-image');
            if (href) {
                window.open(href, '_blank', 'noopener');
            }
        }
    });

    if (detailSummary.editButton) {
        detailSummary.editButton.addEventListener('click', () => {
            const carId = detailSummary.editButton.getAttribute('data-admin-edit') || activeCarId;
            if (detailModal) {
                detailModal.hide();
            }
            if (carId) {
                openEditModal(carId);
            }
        });
    }

    if (editCityInput) {
        const handleCityChange = () => {
            const entry = findCityEntry(editCityInput.value);
            if (entry && Number.isFinite(entry.latitude) && Number.isFinite(entry.longitude)) {
                setEditLocation(entry.latitude, entry.longitude);
            } else {
                updateLocationSummary();
            }
        };
        editCityInput.addEventListener('change', handleCityChange);
        editCityInput.addEventListener('blur', handleCityChange);
    }

    if (editLatInput) {
        editLatInput.addEventListener('change', () => {
            const lat = Number(editLatInput.value);
            const lng = Number(editLngInput ? editLngInput.value : NaN);
            if (Number.isFinite(lat) && Number.isFinite(lng)) {
                setEditLocation(lat, lng, { pan: false });
            } else {
                updateLocationSummary();
            }
        });
    }
    if (editLngInput) {
        editLngInput.addEventListener('change', () => {
            const lat = Number(editLatInput ? editLatInput.value : NaN);
            const lng = Number(editLngInput.value);
            if (Number.isFinite(lat) && Number.isFinite(lng)) {
                setEditLocation(lat, lng, { pan: false });
            } else {
                updateLocationSummary();
            }
        });
    }

    if (editForm) {
        editForm.addEventListener('submit', async (event) => {
            event.preventDefault();
            if (!activeCarId) {
                showFeedback('Vehicle context missing.', 'danger');
                return;
            }
            const formData = new FormData(editForm);
            imagesMarkedForDeletion.forEach((id) => {
                formData.append('delete_images', id);
            });
            try {
                if (submitButton) {
                    submitButton.disabled = true;
                    submitButton.innerHTML = '<span class="spinner-border spinner-border-sm"></span> Saving...';
                }
                const response = await fetch(editForm.action, {
                    method: 'POST',
                    body: formData,
                    headers: { 'X-Requested-With': 'XMLHttpRequest' },
                });
                const payload = await response.json();
                if (!response.ok || !payload || !payload.success) {
                    throw new Error((payload && payload.error) || 'Unable to save vehicle changes.');
                }
                showFeedback('Vehicle updated successfully.');
                const updatedCar = payload.car;
                existingImages = Array.isArray(updatedCar.images) ? updatedCar.images : [];
                imagesMarkedForDeletion.clear();
                renderExistingImages();
                updateCardDisplay(updatedCar);
                setTimeout(() => {
                    if (editModal) {
                        editModal.hide();
                    } else {
                        window.location.reload();
                    }
                }, 900);
            } catch (error) {
                showFeedback(error.message || 'Update failed. Please try again.', 'danger');
            } finally {
                if (submitButton) {
                    submitButton.disabled = false;
                    submitButton.textContent = 'Save changes';
                }
            }
        });
    }
});


