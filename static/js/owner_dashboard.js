document.addEventListener('DOMContentLoaded', () => {
    const MAX_GALLERY_IMAGES = 8;
    const DEFAULT_MAP_VIEW = { lat: 20.5937, lng: 78.9629, zoom: 5 };
    const buildLocationIcon = (className = 'map-pin-car', label = '') => L.divIcon({
        className,
        html: `<span class="map-pin ${label ? 'map-pin-labeled' : ''}">${label}</span>`,
        iconSize: [30, 30],
        iconAnchor: [15, 28],
        popupAnchor: [0, -20],
    });
    const deliveryIcon = buildLocationIcon('map-pin-delivery', 'D');
    const carIcon = buildLocationIcon('map-pin-car', 'C');
    const destinationIcon = (index) => buildLocationIcon('map-pin-destination', String(index + 1));
    const CITY_ENTRIES = Array.isArray(window.OWNER_CITY_ENTRIES) ? window.OWNER_CITY_ENTRIES : [];

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

    const toFixedIfFinite = (value, digits = 6) => {
        const numeric = Number(value);
        return Number.isFinite(numeric) ? numeric.toFixed(digits) : '';
    };

    const createPhotoManager = ({ containerId, addButtonId, inputName, initialMax = MAX_GALLERY_IMAGES }) => {
        const container = document.getElementById(containerId);
        const addButton = document.getElementById(addButtonId);
        if (!container || !addButton) {
            return null;
        }
        let maxInputs = typeof initialMax === 'number' ? initialMax : MAX_GALLERY_IMAGES;

        const getRows = () => Array.from(container.querySelectorAll('[data-role="photo-input-row"]'));

        const updateAddButtonState = () => {
            if (maxInputs <= 0) {
                addButton.disabled = true;
                return;
            }
            addButton.disabled = getRows().length >= maxInputs;
        };

        const buildRow = () => {
            const row = document.createElement('div');
            row.className = 'input-group photo-input-row';
            row.dataset.role = 'photo-input-row';

            const input = document.createElement('input');
            input.type = 'file';
            input.name = inputName;
            input.accept = 'image/*';
            input.className = 'form-control';
            input.addEventListener('change', () => ensureSpare());

            const removeBtn = document.createElement('button');
            removeBtn.type = 'button';
            removeBtn.className = 'btn btn-outline-danger';
            removeBtn.dataset.role = 'remove-photo';
            removeBtn.innerHTML = '<i class="bi bi-x-lg"></i>';
            removeBtn.addEventListener('click', () => {
                const rows = getRows();
                if (rows.length <= 1) {
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
        const rows = getRows();
        const hasEmpty = rows.some((row) => {
            const input = row.querySelector('input[type="file"]');
            return input && input.files.length === 0;
        });
        if (!hasEmpty && rows.length < maxInputs) {
            container.appendChild(buildRow());
        }
        updateAddButtonState();
    };

    const addRow = (focusInput = true) => {
        if (maxInputs > 0 && getRows().length >= maxInputs) {
            updateAddButtonState();
            return;
        }
        const row = buildRow();
        container.appendChild(row);
        updateAddButtonState();
        if (focusInput) {
            const input = row.querySelector('input[type="file"]');
            if (input) {
                input.focus();
            }
        }
    };

    const reset = (newMax = initialMax) => {
        maxInputs = typeof newMax === 'number' ? newMax : initialMax;
        if (maxInputs < 0) {
            maxInputs = 0;
        }
        container.innerHTML = '';
        if (maxInputs > 0) {
            container.appendChild(buildRow());
        }
        updateAddButtonState();
        ensureSpare();
    };

    addButton.addEventListener('click', () => {
        addRow();
        ensureSpare();
    });

    reset(initialMax);

    return {
        reset,
        setMax: (value) => {
            maxInputs = typeof value === 'number' ? value : maxInputs;
            if (maxInputs < 0) {
                maxInputs = 0;
            }
            ensureSpare();
        },
        ensureSpare,
    };
    };

    const addPhotoManager = createPhotoManager({
        containerId: 'photo-input-list',
        addButtonId: 'add-photo-input',
        inputName: 'photos',
        initialMax: MAX_GALLERY_IMAGES,
    });

    const editPhotoManager = createPhotoManager({
        containerId: 'edit-photo-input-list',
        addButtonId: 'add-edit-photo-input',
        inputName: 'new_photos',
        initialMax: MAX_GALLERY_IMAGES,
    });

    const editModalEl = document.getElementById('editCarModal');
    const editForm = document.getElementById('edit-car-form');
    const feedbackEl = document.getElementById('edit-car-feedback');
    const currentImagesContainer = document.getElementById('edit-current-images');
    const photoHelper = document.getElementById('edit-photo-helper');
    const editCityInput = document.getElementById('edit-city');
    const editLatInput = document.getElementById('edit-latitude');
    const editLngInput = document.getElementById('edit-longitude');
    const editHasGpsInput = document.getElementById('edit-has-gps');
    const editImageUrlInput = document.getElementById('edit-image-url');
    const editDescriptionInput = document.getElementById('edit-description');
    const editLocationSummary = document.getElementById('edit-location-summary');
    const editLocationMapEl = document.getElementById('edit-location-map');
    const editModal = editModalEl ? new bootstrap.Modal(editModalEl) : null;
    const modalMarkerIcon = buildLocationIcon();
    const formMarkerIcon = buildLocationIcon();
    const deliveryContainer = document.getElementById('edit-delivery-options');
    const deliveryRows = deliveryContainer ? Array.from(deliveryContainer.querySelectorAll('[data-delivery-distance]')) : [];

    let editMap = null;
    let editMarker = null;
    let modalOpenIntent = null;
    let currentCardElement = null;
    let existingImages = [];
    const imagesMarkedForDeletion = new Set();

    const resetFeedback = () => {
        if (!feedbackEl) {
            return;
        }
        feedbackEl.textContent = '';
        feedbackEl.className = 'alert d-none';
    };

    const showFeedback = (message, type = 'success') => {
        if (!feedbackEl) {
            return;
        }
        feedbackEl.textContent = message;
        feedbackEl.className = `alert alert-${type}`;
    };

    const resetDeliveryRows = () => {
        if (!deliveryRows.length) {
            return;
        }
        deliveryRows.forEach((row, index) => {
            const checkbox = row.querySelector('[data-delivery-checkbox]');
            const priceInput = row.querySelector('[data-delivery-price]');
            if (checkbox) {
                checkbox.checked = false;
            }
            if (priceInput) {
                priceInput.disabled = true;
                priceInput.value = '';
            }
        });
    };

    const applyDeliveryOptions = (options = {}) => {
        if (!deliveryRows.length) {
            return;
        }
        deliveryRows.forEach((row) => {
            const distance = Number(row.dataset.deliveryDistance || '0');
            const checkbox = row.querySelector('[data-delivery-checkbox]');
            const priceInput = row.querySelector('[data-delivery-price]');
            if (!checkbox || !priceInput) {
                return;
            }
            const price = options[distance] ?? options[String(distance)];
            if (price !== undefined) {
                checkbox.checked = true;
                priceInput.disabled = false;
                priceInput.value = price;
            } else {
                checkbox.checked = false;
                priceInput.disabled = true;
                priceInput.value = '';
            }
        });
    };

    if (deliveryRows.length) {
        deliveryRows.forEach((row) => {
            const checkbox = row.querySelector('[data-delivery-checkbox]');
            const priceInput = row.querySelector('[data-delivery-price]');
            if (!checkbox || !priceInput) {
                return;
            }
            checkbox.addEventListener('change', () => {
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
    }

    const addDeliveryContainer = document.getElementById('add-delivery-options');
    if (addDeliveryContainer) {
        addDeliveryContainer.querySelectorAll('[data-distance]').forEach((row) => {
            const checkbox = row.querySelector('[data-add-delivery-checkbox]');
            const priceInput = row.querySelector('[data-add-delivery-price]');
            if (!checkbox || !priceInput) {
                return;
            }
            checkbox.addEventListener('change', () => {
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
    }

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
            if (remaining > 0) {
                editPhotoManager.ensureSpare();
            }
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
            wrapper.className = 'd-flex flex-column align-items-center edit-image-item';
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

            const removeBtn = document.createElement('button');
            removeBtn.type = 'button';
            removeBtn.className = 'btn btn-sm w-100';
            if (imagesMarkedForDeletion.has(image.id)) {
                removeBtn.classList.add('btn-outline-secondary');
                removeBtn.innerHTML = '<i class="bi bi-arrow-counterclockwise me-1"></i>Undo remove';
            } else {
                removeBtn.classList.add('btn-outline-danger');
                removeBtn.innerHTML = '<i class="bi bi-trash me-1"></i>Remove';
            }
            removeBtn.addEventListener('click', () => {
                if (imagesMarkedForDeletion.has(image.id)) {
                    imagesMarkedForDeletion.delete(image.id);
                } else {
                    imagesMarkedForDeletion.add(image.id);
                }
                renderExistingImages();
            });

            wrapper.appendChild(img);
            wrapper.appendChild(removeBtn);
            currentImagesContainer.appendChild(wrapper);
        });
        updateNewPhotoLimit();
    };

    const ensureEditMap = () => {
        if (!editLocationMapEl || typeof L === 'undefined') {
            return;
        }
        if (editMap) {
            return;
        }
        editMap = L.map(editLocationMapEl, { zoomControl: true, scrollWheelZoom: false });
        editMap.setView([DEFAULT_MAP_VIEW.lat, DEFAULT_MAP_VIEW.lng], DEFAULT_MAP_VIEW.zoom);
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            maxZoom: 18,
            attribution: '&copy; <a href="https://www.openstreetmap.org">OpenStreetMap</a> contributors',
        }).addTo(editMap);
        editMarker = L.marker([DEFAULT_MAP_VIEW.lat, DEFAULT_MAP_VIEW.lng], {
            draggable: true,
            icon: modalMarkerIcon,
        }).addTo(editMap);
        editMarker.on('dragend', (event) => {
            const pos = event.target.getLatLng();
            setEditLocation(pos.lat, pos.lng, { pan: false });
        });
        editMap.on('click', (event) => {
            setEditLocation(event.latlng.lat, event.latlng.lng);
        });
        if (editModalEl) {
            editModalEl.addEventListener('shown.bs.modal', () => {
                editMap.invalidateSize();
                if (editMarker) {
                    editMap.panTo(editMarker.getLatLng());
                }
                if (modalOpenIntent === 'location') {
                    editLocationMapEl.scrollIntoView({ behavior: 'smooth', block: 'center' });
                    if (editCityInput) {
                        editCityInput.focus();
                    }
                }
                modalOpenIntent = null;
            });
        }
    };

    const updateLocationSummary = () => {
        if (!editLocationSummary) {
            return;
        }
        const pieces = [];
        if (editCityInput && editCityInput.value.trim()) {
            pieces.push(editCityInput.value.trim());
        }
        if (editLatInput && editLngInput && editLatInput.value && editLngInput.value) {
            pieces.push(`Lat ${toFixedIfFinite(editLatInput.value, 4)}, Lng ${toFixedIfFinite(editLngInput.value, 4)}`);
        }
        editLocationSummary.textContent = pieces.length ? pieces.join('  ') : 'Location not set';
    };

    const setEditLocation = (lat, lng, { pan = true } = {}) => {
        if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
            return;
        }
        if (editLatInput) {
            editLatInput.value = Number(lat).toFixed(6);
        }
        if (editLngInput) {
            editLngInput.value = Number(lng).toFixed(6);
        }
        ensureEditMap();
        if (editMarker) {
            editMarker.setLatLng([lat, lng]);
            editMarker.setIcon(modalMarkerIcon);
        }
        if (pan && editMap) {
            const zoom = editMap.getZoom();
            editMap.setView([lat, lng], Math.max(zoom, 13));
        }
        updateLocationSummary();
    };

    if (editCityInput) {
        const handleEditCityChange = () => {
            const entry = findCityEntry(editCityInput.value);
            if (entry && Number.isFinite(entry.latitude) && Number.isFinite(entry.longitude)) {
                setEditLocation(entry.latitude, entry.longitude);
            } else {
                updateLocationSummary();
            }
        };
        editCityInput.addEventListener('change', handleEditCityChange);
        editCityInput.addEventListener('blur', handleEditCityChange);
    }

    const refreshCarCard = (cardEl, car) => {
        if (!cardEl) {
            return;
        }
        if (car.detail_url) {
            cardEl.setAttribute('data-detail-url', car.detail_url);
        }
        if (car.update_url) {
            cardEl.setAttribute('data-update-url', car.update_url);
        }
        const displayName = car.display_name || car.name || `${car.brand || ''} ${car.model || ''}`.trim();
        const nameEl = cardEl.querySelector('[data-role="car-name"]');
        if (nameEl) {
            nameEl.textContent = displayName;
        }
        const subtitleEl = cardEl.querySelector('[data-role="car-subtitle"]');
        if (subtitleEl) {
            const subtitleName = `${car.brand || 'Unknown'} ${car.model || ''}`.trim();
            subtitleEl.textContent = `${subtitleName} - ${car.city || 'City not set'}`;
        }
        const availabilityEl = cardEl.querySelector('[data-role="car-availability"]');
        if (availabilityEl) {
            const available = Boolean(car.is_available);
            availabilityEl.textContent = available ? 'Available' : 'Unavailable';
            availabilityEl.classList.toggle('bg-success', available);
            availabilityEl.classList.toggle('bg-secondary', !available);
        }
        const licenceEl = cardEl.querySelector('[data-role="car-licence"]');
        if (licenceEl) {
            licenceEl.textContent = `Licence: ${car.licence_plate || ''}`;
        }
        const seatsEl = cardEl.querySelector('[data-role="car-seats"]');
        if (seatsEl) {
            seatsEl.textContent = `Seats: ${car.seats ?? ''}`;
        }
        const rateHourEl = cardEl.querySelector('[data-role="car-rate-hour"]');
        if (rateHourEl) {
            rateHourEl.textContent = `Hourly rate: Rs ${Math.round(car.rate_per_hour ?? 0)}`;
        }
        const rateDayEl = cardEl.querySelector('[data-role="car-rate-day"]');
        if (rateDayEl) {
            rateDayEl.textContent = `Daily rate: Rs ${Math.round(car.daily_rate ?? 0)}`;
        }
        const fuelEl = cardEl.querySelector('[data-role="car-fuel"]');
        if (fuelEl) {
            fuelEl.textContent = `Fuel type: ${car.fuel_type || 'NA'}`;
        }
        const transEl = cardEl.querySelector('[data-role="car-transmission"]');
        if (transEl) {
            transEl.textContent = `Transmission: ${car.transmission || 'NA'}`;
        }
        const locationLabel = cardEl.querySelector('[data-role="car-location-label"]');
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
        const gallery = cardEl.querySelector('[data-role="car-gallery"]');
        if (gallery) {
            gallery.innerHTML = '';
            const images = Array.isArray(car.images) ? car.images : [];
            if (images.length) {
                images.forEach((image) => {
                    const button = document.createElement('button');
                    button.type = 'button';
                    button.className = 'btn btn-link p-0 car-edit-trigger';
                    button.dataset.carId = car.id;
                    const img = document.createElement('img');
                    img.src = image.url || image.filename;
                    img.alt = displayName || 'Vehicle photo';
                    img.className = 'rounded';
                    img.style.width = '96px';
                    img.style.height = '64px';
                    img.style.objectFit = 'cover';
                    button.appendChild(img);
                    gallery.appendChild(button);
                });
            } else {
                const addBtn = document.createElement('button');
                addBtn.type = 'button';
                addBtn.className = 'btn btn-outline-secondary car-edit-trigger';
                addBtn.dataset.carId = car.id;
                addBtn.innerHTML = '<i class="bi bi-plus-circle me-1"></i>Add photos';
                gallery.appendChild(addBtn);
            }
        }
        const toggleBtn = cardEl.querySelector('form[action*="owner_toggle_availability"] button');
        if (toggleBtn) {
            const available = Boolean(car.is_available);
            toggleBtn.textContent = available ? 'Mark unavailable' : 'Mark available';
            toggleBtn.classList.toggle('btn-outline-secondary', available);
            toggleBtn.classList.toggle('btn-outline-success', !available);
        }
        const deliverySummaryEl = cardEl.querySelector('[data-role="car-delivery-summary"]');
        if (deliverySummaryEl) {
            const entries = Object.entries(car.delivery_options || {})
                .map(([distance, price]) => [Number(distance), Number(price)])
                .filter(([distance]) => Number.isFinite(distance))
                .sort((a, b) => a[0] - b[0]);
            if (!entries.length) {
                deliverySummaryEl.textContent = 'Delivery: Not offered';
            } else {
                const parts = entries.map(([distance, price]) => `Up to ${distance} km (₹ ${Math.round(price)})`);
                deliverySummaryEl.textContent = `Delivery: ${parts.join(', ')}`;
            }
        }
    };

    const populateEditForm = (car) => {
        const modalTitle = document.getElementById('edit-car-modal-label');
        if (modalTitle) {
            modalTitle.textContent = car.display_name ? `Manage ${car.display_name}` : 'Manage vehicle';
        }
        const idField = document.getElementById('edit-car-id');
        if (idField) {
            idField.value = car.id || '';
        }
        const setInputValue = (selector, value = '') => {
            const element = document.getElementById(selector);
            if (element) {
                element.value = value || '';
            }
        };
        const setSelectValue = (selector, value = '') => {
            const element = document.getElementById(selector);
            if (!element) {
                return;
            }
            let found = false;
            Array.from(element.options).forEach((option) => {
                if (option.value === value) {
                    found = true;
                }
            });
            if (!found && value) {
                const opt = new Option(value, value, true, true);
                element.appendChild(opt);
            }
            element.value = value || '';
        };

        setSelectValue('edit-vehicle-type', car.vehicle_type || '');
        setInputValue('edit-size-category', car.size_category || '');
        setInputValue('edit-name', car.name || '');
        setInputValue('edit-brand', car.brand || '');
        setInputValue('edit-model', car.model || '');
        setInputValue('edit-licence', car.licence_plate || '');
        setInputValue('edit-seats', car.seats ?? '');
        setInputValue('edit-fuel', car.fuel_type || '');
        setInputValue('edit-transmission', car.transmission || '');
        if (editCityInput) {
            editCityInput.value = car.city || '';
        }
        setInputValue('edit-rate-hour', car.rate_per_hour ?? '');
        setInputValue('edit-rate-day', car.daily_rate ?? '');
        if (editHasGpsInput) {
            editHasGpsInput.checked = !!car.has_gps;
        }
        if (editImageUrlInput) {
            editImageUrlInput.value = car.image_url || '';
        }
        if (editDescriptionInput) {
            editDescriptionInput.value = car.description || '';
        }

        resetDeliveryRows();
        applyDeliveryOptions(car.delivery_options || {});

        ensureEditMap();
        const lat = Number(car.latitude);
        const lng = Number(car.longitude);
        if (Number.isFinite(lat) && Number.isFinite(lng)) {
            setEditLocation(lat, lng);
        } else {
            const entry = findCityEntry(car.city || '');
            if (entry && Number.isFinite(entry.latitude) && Number.isFinite(entry.longitude)) {
                setEditLocation(entry.latitude, entry.longitude, { pan: false });
            } else {
                setEditLocation(DEFAULT_MAP_VIEW.lat, DEFAULT_MAP_VIEW.lng, { pan: false });
            }
        }
        updateLocationSummary();

        existingImages = Array.isArray(car.images) ? car.images : [];
        imagesMarkedForDeletion.clear();
        renderExistingImages();
        if (editPhotoManager) {
            editPhotoManager.reset(MAX_GALLERY_IMAGES);
        }
        updateNewPhotoLimit();
    };

    const openCarModal = async (cardEl, intent = 'manage') => {
        if (!cardEl || !editModal || !editForm) {
            return;
        }
        const detailUrl = cardEl.getAttribute('data-detail-url');
        if (!detailUrl) {
            return;
        }
        currentCardElement = cardEl;
        modalOpenIntent = intent;
        resetFeedback();
        if (editPhotoManager) {
            editPhotoManager.reset(MAX_GALLERY_IMAGES);
        }
        existingImages = [];
        imagesMarkedForDeletion.clear();
        renderExistingImages();
        updateNewPhotoLimit();
        resetDeliveryRows();
        try {
            const response = await fetch(detailUrl, {
                headers: { 'Accept': 'application/json' },
            });
            if (!response.ok) {
                throw new Error('Failed to load');
            }
            const payload = await response.json();
            if (!payload || !payload.car) {
                throw new Error('Missing data');
            }
            populateEditForm(payload.car);
            editModal.show();
        } catch (error) {
            console.error(error);
            alert('Unable to load vehicle details. Please try again.');
        }
    };

    document.addEventListener('click', (event) => {
        const manageTrigger = event.target.closest('.car-edit-trigger');
        if (manageTrigger) {
            const cardEl = manageTrigger.closest('[data-car-card]');
            openCarModal(cardEl, 'manage');
            return;
        }
        const locationTrigger = event.target.closest('.car-location-trigger');
        if (locationTrigger) {
            const cardEl = locationTrigger.closest('[data-car-card]');
            openCarModal(cardEl, 'location');
        }
    });

    if (editForm) {
        editForm.addEventListener('submit', async (event) => {
            event.preventDefault();
            if (!currentCardElement) {
                return;
            }
            const updateUrl = currentCardElement.getAttribute('data-update-url');
            if (!updateUrl) {
                return;
            }
            resetFeedback();
            const formData = new FormData(editForm);
            imagesMarkedForDeletion.forEach((imageId) => {
                formData.append('delete_images', imageId);
            });
            try {
                const response = await fetch(updateUrl, {
                    method: 'POST',
                    body: formData,
                });
                const payload = await response.json();
                if (!response.ok || !payload || !payload.success) {
                    showFeedback((payload && payload.message) || 'Unable to save changes.', 'danger');
                    return;
                }
                const updatedCar = payload.car;
                existingImages = Array.isArray(updatedCar.images) ? updatedCar.images : [];
                imagesMarkedForDeletion.clear();
                renderExistingImages();
                if (editPhotoManager) {
                    editPhotoManager.reset(MAX_GALLERY_IMAGES);
                }
                updateNewPhotoLimit();
                refreshCarCard(currentCardElement, updatedCar);
                showFeedback('Listing updated.', 'success');
                setTimeout(() => {
                    editModal.hide();
                    resetFeedback();
                }, 800);
            } catch (error) {
                console.error(error);
                showFeedback('Unexpected error. Please try again.', 'danger');
            }
        });
    }

    const ownerMapElement = document.getElementById('owner-map');
    const initPendingRequestMaps = () => {
        if (typeof L === 'undefined') {
            return;
        }
        document.querySelectorAll('[data-owner-request-map]').forEach((container) => {
            if (!container || container.dataset.mapReady === '1') {
                return;
            }
            const deliveryLat = Number(container.getAttribute('data-delivery-lat'));
            const deliveryLng = Number(container.getAttribute('data-delivery-lng'));
            if (!Number.isFinite(deliveryLat) || !Number.isFinite(deliveryLng)) {
                return;
            }
            const map = L.map(container, { zoomControl: false, attributionControl: false });
            L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                maxZoom: 18,
                attribution: '&copy; <a href="https://www.openstreetmap.org">OpenStreetMap</a> contributors',
            }).addTo(map);
            const bounds = [];
            const addMarker = (lat, lng, options = {}) => {
                if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
                    return null;
                }
                const marker = L.marker([lat, lng], options).addTo(map);
                bounds.push([lat, lng]);
                return marker;
            };
            addMarker(deliveryLat, deliveryLng, { title: 'Delivery location', icon: deliveryIcon });
            const carLat = Number(container.getAttribute('data-car-lat'));
            const carLng = Number(container.getAttribute('data-car-lng'));
            if (Number.isFinite(carLat) && Number.isFinite(carLng)) {
                addMarker(carLat, carLng, { title: 'Vehicle location', icon: carIcon });
            }
            let destinations = [];
            try {
                destinations = JSON.parse(container.getAttribute('data-destinations') || '[]');
            } catch (error) {
                destinations = [];
            }
            destinations.forEach((destination, index) => {
                const lat = Number(destination.latitude);
                const lng = Number(destination.longitude);
                if (Number.isFinite(lat) && Number.isFinite(lng)) {
                    addMarker(lat, lng, { title: destination.name || 'Destination', icon: destinationIcon(index) });
                }
            });
            if (bounds.length) {
                const latLngBounds = L.latLngBounds(bounds);
                map.fitBounds(latLngBounds.pad(0.25));
            } else {
                map.setView([deliveryLat, deliveryLng], 12);
            }
            setTimeout(() => map.invalidateSize(), 120);
            container.dataset.mapReady = '1';
        });
    };
    initPendingRequestMaps();
    if (ownerMapElement && typeof L !== 'undefined') {
        const cityInput = document.getElementById('city');
        const latitudeField = document.getElementById('latitude');
        const longitudeField = document.getElementById('longitude');
        const map = L.map(ownerMapElement, { zoomControl: true, scrollWheelZoom: false });
        map.setView([DEFAULT_MAP_VIEW.lat, DEFAULT_MAP_VIEW.lng], DEFAULT_MAP_VIEW.zoom);
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            maxZoom: 18,
            attribution: '&copy; <a href="https://www.openstreetmap.org">OpenStreetMap</a> contributors',
        }).addTo(map);
        let marker = null;
        const ensureMapSize = () => map.invalidateSize();
        map.whenReady(() => {
            ensureMapSize();
            setTimeout(ensureMapSize, 150);
        });
        window.addEventListener('resize', ensureMapSize);
        const setMarker = (lat, lng) => {
            if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
                return;
            }
            if (marker) {
                marker.setLatLng([lat, lng]);
                marker.setIcon(formMarkerIcon);
            } else {
                marker = L.marker([lat, lng], { draggable: true, icon: formMarkerIcon }).addTo(map);
                marker.on('dragend', (event) => {
                    const pos = event.target.getLatLng();
                    if (latitudeField) {
                        latitudeField.value = pos.lat.toFixed(6);
                    }
                    if (longitudeField) {
                        longitudeField.value = pos.lng.toFixed(6);
                    }
                });
            }
            if (latitudeField) {
                latitudeField.value = lat.toFixed(6);
            }
            if (longitudeField) {
                longitudeField.value = lng.toFixed(6);
            }
        };
        const focusOn = (lat, lng, zoom = 13) => {
            if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
                return;
            }
            map.setView([lat, lng], zoom);
            setMarker(lat, lng);
        };
        const handleAddCitySelection = () => {
            const entry = findCityEntry(cityInput ? cityInput.value : '');
            if (entry && Number.isFinite(entry.latitude) && Number.isFinite(entry.longitude)) {
                focusOn(entry.latitude, entry.longitude, 11);
            }
        };
        if (cityInput) {
            cityInput.addEventListener('change', handleAddCitySelection);
            cityInput.addEventListener('blur', handleAddCitySelection);
        }
        map.on('click', (event) => {
            focusOn(event.latlng.lat, event.latlng.lng);
        });
        const latExisting = latitudeField ? parseFloat(latitudeField.value) : NaN;
        const lngExisting = longitudeField ? parseFloat(longitudeField.value) : NaN;
        if (Number.isFinite(latExisting) && Number.isFinite(lngExisting)) {
            focusOn(latExisting, lngExisting);
        } else {
            handleAddCitySelection();
        }
    }
});

