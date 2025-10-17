document.addEventListener('DOMContentLoaded', () => {
    const MAX_GALLERY_IMAGES = 8;

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
            const rows = getRows();
            addButton.disabled = rows.length >= maxInputs;
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
            if (maxInputs > 0) {
                const rows = getRows();
                if (rows.length >= maxInputs) {
                    updateAddButtonState();
                    return;
                }
            }
            const newRow = buildRow();
            container.appendChild(newRow);
            updateAddButtonState();
            if (focusInput) {
                const input = newRow.querySelector('input[type="file"]');
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
            container,
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

    createPhotoManager({
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
    const editModal = editModalEl ? new bootstrap.Modal(editModalEl) : null;

    let currentCardElement = null;
    let existingImages = [];
    const imagesMarkedForDeletion = new Set();

    const resetFeedback = () => {
        if (feedbackEl) {
            feedbackEl.textContent = '';
            feedbackEl.className = 'alert d-none';
        }
    };

    const showFeedback = (message, type = 'success') => {
        if (feedbackEl) {
            feedbackEl.textContent = message;
            feedbackEl.className = `alert alert-${type}`;
        }
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
        const allowed = MAX_GALLERY_IMAGES - (existingImages.length - imagesMarkedForDeletion.size);
        if (editPhotoManager) {
            editPhotoManager.setMax(Math.max(allowed, 0));
            if (allowed > 0) {
                editPhotoManager.ensureSpare();
            }
        }
        updatePhotoLimitMessage(Math.max(allowed, 0));
    };

    const renderExistingImages = () => {
        if (!currentImagesContainer) {
            return;
        }
        currentImagesContainer.innerHTML = '';
        if (!existingImages.length) {
            const emptyMessage = document.createElement('p');
            emptyMessage.className = 'text-muted small mb-0';
            emptyMessage.textContent = 'No gallery photos yet.';
            currentImagesContainer.appendChild(emptyMessage);
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

    const setSelectValue = (select, value) => {
        if (!select) {
            return;
        }
        const normalised = value || '';
        let found = false;
        Array.from(select.options).forEach((option) => {
            if (option.value === normalised) {
                found = true;
            }
        });
        if (!found && normalised) {
            const opt = new Option(normalised, normalised, true, true);
            select.appendChild(opt);
        }
        select.value = normalised;
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
        setSelectValue(document.getElementById('edit-vehicle-type'), car.vehicle_type || '');
        const sizeField = document.getElementById('edit-size-category');
        if (sizeField) sizeField.value = car.size_category || '';
        const nameField = document.getElementById('edit-name');
        if (nameField) nameField.value = car.name || '';
        const brandField = document.getElementById('edit-brand');
        if (brandField) brandField.value = car.brand || '';
        const modelField = document.getElementById('edit-model');
        if (modelField) modelField.value = car.model || '';
        const licenceField = document.getElementById('edit-licence');
        if (licenceField) licenceField.value = car.licence_plate || '';
        const seatsField = document.getElementById('edit-seats');
        if (seatsField) seatsField.value = car.seats ?? '';
        const fuelField = document.getElementById('edit-fuel');
        if (fuelField) fuelField.value = car.fuel_type || '';
        const transmissionField = document.getElementById('edit-transmission');
        if (transmissionField) transmissionField.value = car.transmission || '';
        const cityField = document.getElementById('edit-city');
        if (cityField) cityField.value = car.city || '';
        const rateHourField = document.getElementById('edit-rate-hour');
        if (rateHourField) rateHourField.value = car.rate_per_hour ?? '';
        const rateDayField = document.getElementById('edit-rate-day');
        if (rateDayField) rateDayField.value = car.daily_rate ?? '';
        const latField = document.getElementById('edit-latitude');
        if (latField) latField.value = car.latitude ?? '';
        const lngField = document.getElementById('edit-longitude');
        if (lngField) lngField.value = car.longitude ?? '';
        const hasGpsField = document.getElementById('edit-has-gps');
        if (hasGpsField) hasGpsField.checked = !!car.has_gps;
        const imageUrlField = document.getElementById('edit-image-url');
        if (imageUrlField) imageUrlField.value = car.image_url || '';
        const descriptionField = document.getElementById('edit-description');
        if (descriptionField) descriptionField.value = car.description || '';
        existingImages = Array.isArray(car.images) ? car.images : [];
        imagesMarkedForDeletion.clear();
        renderExistingImages();
        if (editPhotoManager) {
            editPhotoManager.reset(MAX_GALLERY_IMAGES);
            updateNewPhotoLimit();
        }
    };

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
        if (licenceEl) licenceEl.textContent = `Licence: ${car.licence_plate || ''}`;
        const seatsEl = cardEl.querySelector('[data-role="car-seats"]');
        if (seatsEl) seatsEl.textContent = `Seats: ${car.seats ?? ''}`;
        const rateHourEl = cardEl.querySelector('[data-role="car-rate-hour"]');
        if (rateHourEl) rateHourEl.textContent = `Hourly rate: Rs ${Math.round(car.rate_per_hour ?? 0)}`;
        const rateDayEl = cardEl.querySelector('[data-role="car-rate-day"]');
        if (rateDayEl) rateDayEl.textContent = `Daily rate: Rs ${Math.round(car.daily_rate ?? 0)}`;
        const fuelEl = cardEl.querySelector('[data-role="car-fuel"]');
        if (fuelEl) fuelEl.textContent = `Fuel type: ${car.fuel_type || 'NA'}`;
        const transEl = cardEl.querySelector('[data-role="car-transmission"]');
        if (transEl) transEl.textContent = `Transmission: ${car.transmission || 'NA'}`;
        const toggleBtn = cardEl.querySelector('form[action*="owner_toggle_availability"] button');
        if (toggleBtn) {
            const available = Boolean(car.is_available);
            toggleBtn.textContent = available ? 'Mark unavailable' : 'Mark available';
            toggleBtn.classList.toggle('btn-outline-secondary', available);
            toggleBtn.classList.toggle('btn-outline-success', !available);
        }
        const latInput = cardEl.querySelector('form[action*="owner_update_location"] input[name="latitude"]');
        if (latInput && car.latitude !== undefined && car.latitude !== null) {
            latInput.value = car.latitude;
        }
        const lngInput = cardEl.querySelector('form[action*="owner_update_location"] input[name="longitude"]');
        if (lngInput && car.longitude !== undefined && car.longitude !== null) {
            lngInput.value = car.longitude;
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
                const addButton = document.createElement('button');
                addButton.type = 'button';
                addButton.className = 'btn btn-outline-secondary car-edit-trigger';
                addButton.dataset.carId = car.id;
                addButton.innerHTML = '<i class="bi bi-plus-circle me-1"></i>Add photos';
                gallery.appendChild(addButton);
            }
        }
    };

    document.addEventListener('click', async (event) => {
        const trigger = event.target.closest('.car-edit-trigger');
        if (!trigger) {
            return;
        }
        const cardEl = trigger.closest('[data-car-card]');
        if (!cardEl || !editModal || !editForm) {
            return;
        }
        event.preventDefault();
        const detailUrl = cardEl.getAttribute('data-detail-url');
        if (!detailUrl) {
            return;
        }
        currentCardElement = cardEl;
        resetFeedback();
        if (editPhotoManager) {
            editPhotoManager.reset(MAX_GALLERY_IMAGES);
        }
        existingImages = [];
        imagesMarkedForDeletion.clear();
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
                    updateNewPhotoLimit();
                }
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

    const mapElement = document.getElementById('owner-map');
    if (mapElement && typeof L !== 'undefined') {
        const cityEntries = window.OWNER_CITY_ENTRIES || [];
        const cityInput = document.getElementById('city');
        const latitudeField = document.getElementById('latitude');
        const longitudeField = document.getElementById('longitude');
        const buildLabel = (entry) => {
            const name = (entry.name || '').trim();
            const state = (entry.state || '').trim();
            return state ? `${name}, ${state}` : name;
        };
        const findCity = (label) => {
            if (!label) {
                return null;
            }
            const normalised = label.trim().toLowerCase();
            return cityEntries.find((entry) => buildLabel(entry).toLowerCase() === normalised) || null;
        };
        const DEFAULT_VIEW = { lat: 20.5937, lng: 78.9629, zoom: 5 };
        const map = L.map(mapElement, { zoomControl: true });
        map.setView([DEFAULT_VIEW.lat, DEFAULT_VIEW.lng], DEFAULT_VIEW.zoom);
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
            } else {
                marker = L.marker([lat, lng], { draggable: true }).addTo(map);
                marker.on('dragend', (event) => {
                    const pos = event.target.getLatLng();
                    if (latitudeField) latitudeField.value = pos.lat.toFixed(6);
                    if (longitudeField) longitudeField.value = pos.lng.toFixed(6);
                });
            }
            if (latitudeField) latitudeField.value = lat.toFixed(6);
            if (longitudeField) longitudeField.value = lng.toFixed(6);
        };
        const focusOn = (lat, lng, zoom = 13) => {
            if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
                return;
            }
            map.setView([lat, lng], zoom);
            setMarker(lat, lng);
        };
        const handleCitySelection = () => {
            const entry = findCity(cityInput ? cityInput.value : '');
            if (entry && Number.isFinite(entry.latitude) && Number.isFinite(entry.longitude)) {
                focusOn(entry.latitude, entry.longitude, 11);
            }
        };
        if (cityInput) {
            cityInput.addEventListener('change', handleCitySelection);
            cityInput.addEventListener('blur', handleCitySelection);
        }
        map.on('click', (event) => {
            focusOn(event.latlng.lat, event.latlng.lng);
        });
        const latExisting = latitudeField ? parseFloat(latitudeField.value) : NaN;
        const lngExisting = longitudeField ? parseFloat(longitudeField.value) : NaN;
        if (Number.isFinite(latExisting) && Number.isFinite(lngExisting)) {
            focusOn(latExisting, lngExisting);
        } else {
            handleCitySelection();
        }
    }
});
