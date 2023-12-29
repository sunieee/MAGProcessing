let papers, links, field_leaves;
let topic2color = {};
let authorID;
let minYear, maxYear;
let svg;
let simulation;
let nodes, edges;
let width, height;
let basedir = '../create_field/out/fellow/'

async function loadData() {
    papers = await d3.csv(`${basedir}/papers/${authorID}.csv`);
    links = await d3.csv(`${basedir}/links/${authorID}.csv`);
    field_leaves = await d3.csv('field_leaves.csv');
    paperID2topic = await d3.json('paperID2topic.json');

    // After loading data, initialize the graph
    preprocessData();
    initializeGraph();
}

function hsvToRgb(h, s, v) {
    const c = v * s;
    const x = c * (1 - Math.abs(((h / 60) % 2) - 1));
    const m = v - c;
    let r, g, b;

    if (h >= 0 && h < 60) {
        [r, g, b] = [c, x, 0];
    } else if (h >= 60 && h < 120) {
        [r, g, b] = [x, c, 0];
    } else if (h >= 120 && h < 180) {
        [r, g, b] = [0, c, x];
    } else if (h >= 180 && h < 240) {
        [r, g, b] = [0, x, c];
    } else if (h >= 240 && h < 300) {
        [r, g, b] = [x, 0, c];
    } else {
        [r, g, b] = [c, 0, x];
    }

    const rgbColor = [(r + m) * 255, (g + m) * 255, (b + m) * 255];
    return rgbColor;
}

function hsvToHex(h, s, v) {
    let rgbColor = hsvToRgb(h, s, v);
    let r = Math.round(rgbColor[0]);
    let g = Math.round(rgbColor[1]);
    let b = Math.round(rgbColor[2]);

    // 将RGB值转换为十六进制字符串
    const toHex = (value) => {
        const hex = value.toString(16);
        return hex.length === 1 ? "0" + hex : hex;
    };

    const red = toHex(r);
    const green = toHex(g);
    const blue = toHex(b);

    return "#" + red + green + blue;
}

function preprocessData() {
    // Assuming field_leaves is an array of objects with a 'Topic' property
    field_leaves.forEach(d => {
        topic2color[d.Topic] = hsvToHex(d.h, 0.7, d.v);
    });

    links.forEach(link => {
        link.source = link.childrenID;
        link.target = link.parentID;
    });

    papers.forEach(paper => {
        if (paper.citationCountByYear === '' || paper.citationCountByYear === undefined) {
            paper.cumulativeCitationsByYear = [];
        } else {
            const citations = paper.citationCountByYear.split(',').map(Number);
            let cumulativeCount = 0;

            paper.cumulativeCitationsByYear = citations.map(count => {
                cumulativeCount += count;
                return cumulativeCount;
            });
        }
        
    })
}

// Define drag event functions
function dragStarted(event, d) {
    if (!event.active) simulation.alphaTarget(0.3).restart();
    d.fx = d.x;
    d.fy = d.y;
}

function dragged(event, d) {
    d.fx = event.x;
    d.fy = event.y;
}

function dragEnded(event, d) {
    if (!event.active) simulation.alphaTarget(0);
    d.fx = null;
    d.fy = null;
}

function initializeGraph() {
    // Determine the width and height for the graph canvas
    const width = 2000;
    const height = 1200;

    // Create an SVG element to hold the graph
    const svg = d3.select('#graph-container')
        .append('svg')
        .attr('width', width)
        .attr('height', height);

    // Setup a simulation with forces
    const simulation = d3.forceSimulation(papers)
        .force('link', d3.forceLink(links).id(d => d.paperID))
        .force('charge', d3.forceManyBody())
        .force('center', d3.forceCenter(width / 2, height / 2));

    // Create edges (lines)
    const edges = svg.append('g')
        .selectAll('line')
        .data(links)
        .enter().append('line')
        .attr('stroke', '#999999')
        .style('stroke-width', d => d.extendsProb); // Edge width based on extendsProb

    // Create nodes (circles)
    const nodes = svg.append('g')
        .selectAll('circle')
        .data(papers)
        .enter().append('circle')
        .attr('opacity', 0.7)
        .attr('r', d => Math.sqrt(d.citationCount) || 5) // Node size based on citationCount
        .attr('fill', d => topic2color[d.topic]) // Node color based on topic
        .call(d3.drag() // Enable node dragging
            .on('start', dragStarted)
            .on('drag', dragged)
            .on('end', dragEnded));

    // Define drag event functions
    function dragStarted(event, d) {
        if (!event.active) simulation.alphaTarget(0.3).restart();
        d.fx = d.x;
        d.fy = d.y;
    }

    function dragged(event, d) {
        d.fx = event.x;
        d.fy = event.y;
    }

    function dragEnded(event, d) {
        if (!event.active) simulation.alphaTarget(0);
        d.fx = null;
        d.fy = null;
    }

    // Update positions each tick
    simulation.on('tick', () => {
        edges
            .attr('x1', d => d.source.x)
            .attr('y1', d => d.source.y)
            .attr('x2', d => d.target.x)
            .attr('y2', d => d.target.y);

        nodes
            .attr('cx', d => d.x)
            .attr('cy', d => d.y);
    });
}



function createTimeline() {
    const timelineContainer = d3.select('#timeline-container');

    for (let year = minYear; year <= maxYear; year++) {
        timelineContainer.append('button')
            .text(year)
            .on('click', () => updateGraph(year));
    }
}

// Listen for the DOMContentLoaded event to ensure the DOM is fully loaded
document.addEventListener('DOMContentLoaded', () => {
    const urlParams = new URLSearchParams(window.location.search);
    authorID = urlParams.get('authorID'); // 获取名为'authorID'的参数
    if (authorID == null) {
        authorID = '2121939561'; // 默认作者ID
    }

    // 可以在这里根据authorID来执行其他操作
    loadData();
});
