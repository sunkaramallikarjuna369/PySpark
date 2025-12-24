// PySpark & Data Engineering Learning Hub - 3D Visualization Library
// Uses Three.js for 3D rendering

class DataVisualization {
    constructor(containerId, options = {}) {
        this.container = document.getElementById(containerId);
        if (!this.container) {
            console.error(`Container ${containerId} not found`);
            return;
        }

        this.options = {
            backgroundColor: 0x1a1a2e,
            cameraPosition: { x: 0, y: 0, z: 5 },
            enableControls: true,
            enableAnimation: true,
            ...options
        };

        this.scene = null;
        this.camera = null;
        this.renderer = null;
        this.controls = null;
        this.animationId = null;
        this.objects = [];

        this.init();
    }

    init() {
        // Scene
        this.scene = new THREE.Scene();
        this.scene.background = new THREE.Color(this.options.backgroundColor);

        // Camera
        const aspect = this.container.clientWidth / this.container.clientHeight;
        this.camera = new THREE.PerspectiveCamera(75, aspect, 0.1, 1000);
        this.camera.position.set(
            this.options.cameraPosition.x,
            this.options.cameraPosition.y,
            this.options.cameraPosition.z
        );

        // Renderer
        this.renderer = new THREE.WebGLRenderer({ antialias: true });
        this.renderer.setSize(this.container.clientWidth, this.container.clientHeight);
        this.renderer.setPixelRatio(window.devicePixelRatio);
        this.container.appendChild(this.renderer.domElement);

        // Controls
        if (this.options.enableControls && typeof THREE.OrbitControls !== 'undefined') {
            this.controls = new THREE.OrbitControls(this.camera, this.renderer.domElement);
            this.controls.enableDamping = true;
            this.controls.dampingFactor = 0.05;
        }

        // Lights
        this.addLights();

        // Handle resize
        window.addEventListener('resize', () => this.onResize());

        // Start animation
        if (this.options.enableAnimation) {
            this.animate();
        }
    }

    addLights() {
        const ambientLight = new THREE.AmbientLight(0xffffff, 0.5);
        this.scene.add(ambientLight);

        const directionalLight = new THREE.DirectionalLight(0xffffff, 0.8);
        directionalLight.position.set(5, 5, 5);
        this.scene.add(directionalLight);

        const pointLight = new THREE.PointLight(0x4dabf7, 0.5);
        pointLight.position.set(-5, 5, -5);
        this.scene.add(pointLight);
    }

    onResize() {
        const width = this.container.clientWidth;
        const height = this.container.clientHeight;

        this.camera.aspect = width / height;
        this.camera.updateProjectionMatrix();
        this.renderer.setSize(width, height);
    }

    animate() {
        this.animationId = requestAnimationFrame(() => this.animate());

        // Update controls
        if (this.controls) {
            this.controls.update();
        }

        // Animate objects
        this.objects.forEach(obj => {
            if (obj.userData.animate) {
                obj.userData.animate(obj);
            }
        });

        this.renderer.render(this.scene, this.camera);
    }

    // Create a data node (cube or sphere)
    createDataNode(options = {}) {
        const {
            type = 'cube',
            size = 0.5,
            color = 0x4dabf7,
            position = { x: 0, y: 0, z: 0 },
            label = '',
            animate = null
        } = options;

        let geometry, material, mesh;

        if (type === 'cube') {
            geometry = new THREE.BoxGeometry(size, size, size);
        } else if (type === 'sphere') {
            geometry = new THREE.SphereGeometry(size / 2, 32, 32);
        } else if (type === 'cylinder') {
            geometry = new THREE.CylinderGeometry(size / 2, size / 2, size, 32);
        }

        material = new THREE.MeshPhongMaterial({
            color: color,
            transparent: true,
            opacity: 0.9,
            shininess: 100
        });

        mesh = new THREE.Mesh(geometry, material);
        mesh.position.set(position.x, position.y, position.z);
        mesh.userData = { label, animate };

        this.scene.add(mesh);
        this.objects.push(mesh);

        return mesh;
    }

    // Create a connection line between two points
    createConnection(start, end, options = {}) {
        const {
            color = 0x00ff00,
            lineWidth = 2,
            dashed = false,
            animated = false
        } = options;

        const points = [
            new THREE.Vector3(start.x, start.y, start.z),
            new THREE.Vector3(end.x, end.y, end.z)
        ];

        const geometry = new THREE.BufferGeometry().setFromPoints(points);
        
        let material;
        if (dashed) {
            material = new THREE.LineDashedMaterial({
                color: color,
                dashSize: 0.1,
                gapSize: 0.05
            });
        } else {
            material = new THREE.LineBasicMaterial({ color: color });
        }

        const line = new THREE.Line(geometry, material);
        if (dashed) {
            line.computeLineDistances();
        }

        this.scene.add(line);
        this.objects.push(line);

        return line;
    }

    // Create an arrow
    createArrow(from, to, options = {}) {
        const {
            color = 0x00ff00,
            headLength = 0.2,
            headWidth = 0.1
        } = options;

        const direction = new THREE.Vector3(
            to.x - from.x,
            to.y - from.y,
            to.z - from.z
        );
        const length = direction.length();
        direction.normalize();

        const origin = new THREE.Vector3(from.x, from.y, from.z);
        const arrow = new THREE.ArrowHelper(direction, origin, length, color, headLength, headWidth);

        this.scene.add(arrow);
        this.objects.push(arrow);

        return arrow;
    }

    // Create a text label (using sprite)
    createLabel(text, position, options = {}) {
        const {
            fontSize = 48,
            fontColor = '#ffffff',
            backgroundColor = 'rgba(0, 0, 0, 0.7)'
        } = options;

        const canvas = document.createElement('canvas');
        const context = canvas.getContext('2d');
        canvas.width = 256;
        canvas.height = 64;

        context.fillStyle = backgroundColor;
        context.fillRect(0, 0, canvas.width, canvas.height);

        context.font = `${fontSize}px Arial`;
        context.fillStyle = fontColor;
        context.textAlign = 'center';
        context.textBaseline = 'middle';
        context.fillText(text, canvas.width / 2, canvas.height / 2);

        const texture = new THREE.CanvasTexture(canvas);
        const material = new THREE.SpriteMaterial({ map: texture });
        const sprite = new THREE.Sprite(material);
        sprite.position.set(position.x, position.y, position.z);
        sprite.scale.set(1, 0.25, 1);

        this.scene.add(sprite);
        this.objects.push(sprite);

        return sprite;
    }

    // Create a data flow animation
    createDataFlow(path, options = {}) {
        const {
            color = 0x00ff00,
            speed = 0.02,
            particleCount = 10
        } = options;

        const particles = [];
        const geometry = new THREE.SphereGeometry(0.05, 8, 8);
        const material = new THREE.MeshBasicMaterial({ color: color });

        for (let i = 0; i < particleCount; i++) {
            const particle = new THREE.Mesh(geometry, material);
            particle.userData = {
                pathIndex: i / particleCount,
                path: path,
                speed: speed,
                animate: (obj) => {
                    obj.userData.pathIndex += obj.userData.speed;
                    if (obj.userData.pathIndex > 1) {
                        obj.userData.pathIndex = 0;
                    }
                    const idx = Math.floor(obj.userData.pathIndex * (path.length - 1));
                    const nextIdx = Math.min(idx + 1, path.length - 1);
                    const t = (obj.userData.pathIndex * (path.length - 1)) % 1;
                    
                    obj.position.lerpVectors(
                        new THREE.Vector3(path[idx].x, path[idx].y, path[idx].z),
                        new THREE.Vector3(path[nextIdx].x, path[nextIdx].y, path[nextIdx].z),
                        t
                    );
                }
            };
            this.scene.add(particle);
            this.objects.push(particle);
            particles.push(particle);
        }

        return particles;
    }

    // Create a grid
    createGrid(size = 10, divisions = 10, options = {}) {
        const { color1 = 0x444444, color2 = 0x888888 } = options;
        const grid = new THREE.GridHelper(size, divisions, color1, color2);
        grid.position.y = -2;
        this.scene.add(grid);
        return grid;
    }

    // Create a plane
    createPlane(width, height, options = {}) {
        const {
            color = 0x333333,
            position = { x: 0, y: 0, z: 0 },
            rotation = { x: -Math.PI / 2, y: 0, z: 0 }
        } = options;

        const geometry = new THREE.PlaneGeometry(width, height);
        const material = new THREE.MeshPhongMaterial({
            color: color,
            side: THREE.DoubleSide,
            transparent: true,
            opacity: 0.5
        });

        const plane = new THREE.Mesh(geometry, material);
        plane.position.set(position.x, position.y, position.z);
        plane.rotation.set(rotation.x, rotation.y, rotation.z);

        this.scene.add(plane);
        this.objects.push(plane);

        return plane;
    }

    // Clear all objects
    clear() {
        this.objects.forEach(obj => {
            this.scene.remove(obj);
            if (obj.geometry) obj.geometry.dispose();
            if (obj.material) obj.material.dispose();
        });
        this.objects = [];
    }

    // Destroy the visualization
    destroy() {
        if (this.animationId) {
            cancelAnimationFrame(this.animationId);
        }
        this.clear();
        if (this.renderer) {
            this.renderer.dispose();
            this.container.removeChild(this.renderer.domElement);
        }
    }
}

// Preset visualizations for common data engineering concepts
const DataVizPresets = {
    // RDD Transformation visualization
    rddTransformation(viz) {
        viz.clear();
        
        // Source RDD
        const sourceNodes = [];
        for (let i = 0; i < 4; i++) {
            const node = viz.createDataNode({
                type: 'cube',
                size: 0.4,
                color: 0xe25a1c,
                position: { x: -3, y: 1 - i * 0.8, z: 0 }
            });
            sourceNodes.push(node);
        }
        viz.createLabel('Source RDD', { x: -3, y: 2, z: 0 });

        // Transformation
        const transformNode = viz.createDataNode({
            type: 'cylinder',
            size: 0.6,
            color: 0x4dabf7,
            position: { x: 0, y: 0, z: 0 },
            animate: (obj) => {
                obj.rotation.y += 0.02;
            }
        });
        viz.createLabel('map()', { x: 0, y: 1.5, z: 0 });

        // Result RDD
        const resultNodes = [];
        for (let i = 0; i < 4; i++) {
            const node = viz.createDataNode({
                type: 'cube',
                size: 0.4,
                color: 0x198754,
                position: { x: 3, y: 1 - i * 0.8, z: 0 }
            });
            resultNodes.push(node);
        }
        viz.createLabel('Result RDD', { x: 3, y: 2, z: 0 });

        // Connections
        sourceNodes.forEach(node => {
            viz.createArrow(
                { x: node.position.x + 0.3, y: node.position.y, z: 0 },
                { x: -0.5, y: 0, z: 0 },
                { color: 0x888888 }
            );
        });

        resultNodes.forEach(node => {
            viz.createArrow(
                { x: 0.5, y: 0, z: 0 },
                { x: node.position.x - 0.3, y: node.position.y, z: 0 },
                { color: 0x888888 }
            );
        });

        // Data flow
        viz.createDataFlow([
            { x: -3, y: 0, z: 0 },
            { x: 0, y: 0, z: 0 },
            { x: 3, y: 0, z: 0 }
        ], { color: 0x00ff00, particleCount: 5 });
    },

    // DataFrame Join visualization
    dataFrameJoin(viz) {
        viz.clear();

        // Left DataFrame
        for (let i = 0; i < 3; i++) {
            viz.createDataNode({
                type: 'cube',
                size: 0.5,
                color: 0x150458,
                position: { x: -2.5, y: 1 - i, z: -1 }
            });
        }
        viz.createLabel('Left DF', { x: -2.5, y: 2, z: -1 });

        // Right DataFrame
        for (let i = 0; i < 3; i++) {
            viz.createDataNode({
                type: 'cube',
                size: 0.5,
                color: 0x336791,
                position: { x: -2.5, y: 1 - i, z: 1 }
            });
        }
        viz.createLabel('Right DF', { x: -2.5, y: 2, z: 1 });

        // Join operation
        const joinNode = viz.createDataNode({
            type: 'sphere',
            size: 1,
            color: 0x8b5cf6,
            position: { x: 0, y: 0, z: 0 },
            animate: (obj) => {
                obj.rotation.y += 0.01;
                obj.scale.x = 1 + Math.sin(Date.now() * 0.002) * 0.1;
                obj.scale.y = 1 + Math.sin(Date.now() * 0.002) * 0.1;
                obj.scale.z = 1 + Math.sin(Date.now() * 0.002) * 0.1;
            }
        });
        viz.createLabel('JOIN', { x: 0, y: 1.5, z: 0 });

        // Result DataFrame
        for (let i = 0; i < 4; i++) {
            viz.createDataNode({
                type: 'cube',
                size: 0.6,
                color: 0x198754,
                position: { x: 2.5, y: 1.5 - i, z: 0 }
            });
        }
        viz.createLabel('Result DF', { x: 2.5, y: 2.5, z: 0 });

        // Arrows
        viz.createArrow({ x: -2, y: 0, z: -1 }, { x: -0.6, y: 0, z: -0.3 }, { color: 0x150458 });
        viz.createArrow({ x: -2, y: 0, z: 1 }, { x: -0.6, y: 0, z: 0.3 }, { color: 0x336791 });
        viz.createArrow({ x: 0.6, y: 0, z: 0 }, { x: 2, y: 0, z: 0 }, { color: 0x198754 });
    },

    // ETL Pipeline visualization
    etlPipeline(viz) {
        viz.clear();

        // Extract sources
        const sources = [
            { x: -4, y: 1.5, z: 0, color: 0xe25a1c, label: 'Database' },
            { x: -4, y: 0, z: 0, color: 0x336791, label: 'API' },
            { x: -4, y: -1.5, z: 0, color: 0x150458, label: 'Files' }
        ];

        sources.forEach(src => {
            viz.createDataNode({
                type: 'cylinder',
                size: 0.6,
                color: src.color,
                position: { x: src.x, y: src.y, z: src.z }
            });
        });
        viz.createLabel('EXTRACT', { x: -4, y: 2.5, z: 0 });

        // Transform
        viz.createDataNode({
            type: 'cube',
            size: 1,
            color: 0x00a86b,
            position: { x: 0, y: 0, z: 0 },
            animate: (obj) => {
                obj.rotation.x += 0.01;
                obj.rotation.y += 0.01;
            }
        });
        viz.createLabel('TRANSFORM', { x: 0, y: 1.5, z: 0 });

        // Load targets
        const targets = [
            { x: 4, y: 0.75, z: 0, color: 0x8b5cf6, label: 'Data Warehouse' },
            { x: 4, y: -0.75, z: 0, color: 0x4dabf7, label: 'Data Lake' }
        ];

        targets.forEach(tgt => {
            viz.createDataNode({
                type: 'cylinder',
                size: 0.7,
                color: tgt.color,
                position: { x: tgt.x, y: tgt.y, z: tgt.z }
            });
        });
        viz.createLabel('LOAD', { x: 4, y: 2, z: 0 });

        // Connections
        sources.forEach(src => {
            viz.createArrow(
                { x: src.x + 0.5, y: src.y, z: src.z },
                { x: -0.7, y: 0, z: 0 },
                { color: 0x888888 }
            );
        });

        targets.forEach(tgt => {
            viz.createArrow(
                { x: 0.7, y: 0, z: 0 },
                { x: tgt.x - 0.5, y: tgt.y, z: tgt.z },
                { color: 0x888888 }
            );
        });

        // Data flow
        viz.createDataFlow([
            { x: -4, y: 0, z: 0 },
            { x: -2, y: 0, z: 0 },
            { x: 0, y: 0, z: 0 },
            { x: 2, y: 0, z: 0 },
            { x: 4, y: 0, z: 0 }
        ], { color: 0x00ff00, particleCount: 8 });
    },

    // Star Schema visualization
    starSchema(viz) {
        viz.clear();

        // Fact table (center)
        viz.createDataNode({
            type: 'cube',
            size: 1.2,
            color: 0xe25a1c,
            position: { x: 0, y: 0, z: 0 },
            animate: (obj) => {
                obj.rotation.y += 0.005;
            }
        });
        viz.createLabel('FACT', { x: 0, y: 1.2, z: 0 });

        // Dimension tables
        const dimensions = [
            { angle: 0, label: 'Time', color: 0x4dabf7 },
            { angle: 72, label: 'Product', color: 0x198754 },
            { angle: 144, label: 'Customer', color: 0x8b5cf6 },
            { angle: 216, label: 'Location', color: 0x336791 },
            { angle: 288, label: 'Channel', color: 0x150458 }
        ];

        const radius = 3;
        dimensions.forEach(dim => {
            const rad = (dim.angle * Math.PI) / 180;
            const x = Math.cos(rad) * radius;
            const z = Math.sin(rad) * radius;

            viz.createDataNode({
                type: 'cube',
                size: 0.8,
                color: dim.color,
                position: { x, y: 0, z }
            });
            viz.createLabel(dim.label, { x, y: 1, z });

            // Connection to fact
            viz.createConnection(
                { x: 0, y: 0, z: 0 },
                { x, y: 0, z },
                { color: 0x888888 }
            );
        });

        viz.createGrid(8, 8);
    },

    // Window Function visualization
    windowFunction(viz) {
        viz.clear();

        // Data rows
        const rows = [];
        for (let i = 0; i < 6; i++) {
            const row = viz.createDataNode({
                type: 'cube',
                size: 0.4,
                color: 0x336791,
                position: { x: -2, y: 2 - i * 0.8, z: 0 }
            });
            rows.push(row);
        }
        viz.createLabel('Data', { x: -2, y: 3, z: 0 });

        // Window frame
        const windowFrame = viz.createDataNode({
            type: 'cube',
            size: 0.1,
            color: 0xffc107,
            position: { x: -2, y: 1.2, z: 0.5 }
        });
        
        // Create window boundary
        const windowGeometry = new THREE.BoxGeometry(0.6, 2.4, 0.1);
        const windowMaterial = new THREE.MeshBasicMaterial({
            color: 0xffc107,
            transparent: true,
            opacity: 0.3,
            wireframe: true
        });
        const windowMesh = new THREE.Mesh(windowGeometry, windowMaterial);
        windowMesh.position.set(-2, 0.4, 0.3);
        windowMesh.userData = {
            animate: (obj) => {
                obj.position.y = 0.4 + Math.sin(Date.now() * 0.001) * 0.8;
            }
        };
        viz.scene.add(windowMesh);
        viz.objects.push(windowMesh);

        viz.createLabel('Window', { x: -2, y: -1.5, z: 0.5 });

        // Result
        for (let i = 0; i < 6; i++) {
            viz.createDataNode({
                type: 'cube',
                size: 0.4,
                color: 0x198754,
                position: { x: 2, y: 2 - i * 0.8, z: 0 }
            });
        }
        viz.createLabel('Result', { x: 2, y: 3, z: 0 });

        // Arrow
        viz.createArrow({ x: -1.5, y: 0.5, z: 0 }, { x: 1.5, y: 0.5, z: 0 }, { color: 0x888888 });
        viz.createLabel('ROW_NUMBER()', { x: 0, y: 1.5, z: 0 });
    }
};

// Export for global use
window.DataVisualization = DataVisualization;
window.DataVizPresets = DataVizPresets;
