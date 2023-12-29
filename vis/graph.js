let papers, links, field_leaves;
let topic2color = {};
let authorID;
let minYear, maxYear;
let svg;
let simulation;
let nodes, edges;
let width, height;
let paperID2topic;
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
    console.log('topic2color', topic2color)

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
        paper.radius = 5 + Math.sqrt(Math.sqrt(paper.totalCitationCount))
        paper.topic = paperID2topic[paper.paperID]
        paper.color = topic2color[paper.topic] || '#000000';
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
    width = 2000;
    height = 1200;

    // Create an SVG element to hold the graph
    svg = d3.select('#graph-container')
        .append('svg')
        .attr('width', width)
        .attr('height', height);

    console.log('papers', papers);
    console.log('links', links);

    minYear = papers.map(p => p.citeStartYear).filter(x=> x>0).reduce((a, b) => Math.min(a, b));
    maxYear = papers.map(p => p.citeEndYear).reduce((a, b) => Math.max(a, b));
    console.log(minYear, maxYear);
    // updateGraph(maxYear);

    simulation = d3.forceSimulation(papers)
        .force('link', d3.forceLink(links).id(d => d.paperID))
        .force('charge', d3.forceManyBody())
        .force('center', d3.forceCenter(width / 2, height / 2));

    // updateNodesAndLinksDisplay(papers, links, false);
    nodes = svg
        .selectAll('circle')
        .data(papers, d => d.paperID)
        .enter()
        .append('circle')
        .attr('fill', d => d.color) // Node color based on topic
        .attr('opacity', 0.7)
        .call(d3.drag() // Enable node dragging
            .on('start', dragStarted)
            .on('drag', dragged)
            .on('end', dragEnded))
        .attr('r', d => d.radius);

    edges = svg
        .selectAll('line')
        .data(links, d => d.childrenID + '-' + d.parentID)
        .enter()
        .append('line')
        .attr('stroke', '#999999')
        .attr('x1', d => d.source.x)
        .attr('y1', d => d.source.y)
        .attr('x2', d => d.target.x)
        .attr('y2', d => d.target.y)
        .style('stroke-width', d => d.extendsProb);

    simulation.on('tick', () => {
        nodes
            .attr('cx', d => d.x)
            .attr('cy', d => d.y);

        edges
            .attr('x1', d => d.source.x)
            .attr('y1', d => d.source.y)
            .attr('x2', d => d.target.x)
            .attr('y2', d => d.target.y);

    });
    
    createTimeline()
}


function updateGraph(year) {
    // Implement the logic to update the graph based on the selected year here
    // ...
    // 过滤节点
    const filteredPapers = papers.filter(paper => paper.year <= year);

    // 过滤边
    // 只保留两端都在filteredPapers中的边
    const filteredLinks = []
    links.forEach(link => {
        if (filteredPapers.find(paper => paper.paperID === link.childrenID)
            && filteredPapers.find(paper => paper.paperID === link.parentID)) {
            filteredLinks.push(link);
        }
    })

    filteredPapers.forEach(paper => {
        const yearIndex = year - paper.citeStartYear;

        let l = paper.cumulativeCitationsByYear.length
        let cumulativeCount = yearIndex < l? paper.cumulativeCitationsByYear[yearIndex] : paper.cumulativeCitationsByYear[l-1];
        cumulativeCount = cumulativeCount || 0;
        
        paper.radius = 5 + Math.sqrt(cumulativeCount);
    });

    console.log(year, filteredPapers, filteredLinks)

    updateNodesAndLinksDisplay(filteredPapers, filteredLinks);

}

function updateNodesAndLinksDisplay(filteredPapers, filteredLinks) {
    // 更新节点
    nodes = svg
        .selectAll('circle')
        .data(filteredPapers, d => d.paperID);
        
    nodes.enter()
        .append('circle')
        .attr('cx', width / 2)
        .attr('cy', height / 2)
        .attr('fill', d => topic2color[paperID2topic[d.paperID]]  || '#000000') // Node color based on topic
        .attr('opacity', 0.7)
        .call(d3.drag() // Enable node dragging
            .on('start', dragStarted)
            .on('drag', dragged)
            .on('end', dragEnded))
        .merge(nodes) // 合并新添加的节点和现有的节点
        .attr('r', d => d.radius);
        
    nodes.exit().remove(); // 移除不再需要的节点

    // 更新链接
    edges = svg
        .selectAll('line')
        .data(filteredLinks, d => d.childrenID + '-' + d.parentID);

    edges.enter()
        .append('line')
        .attr('stroke', '#999999')
        .attr('x1', d => d.source.x)
        .attr('y1', d => d.source.y)
        .attr('x2', d => d.target.x)
        .attr('y2', d => d.target.y)
        // .merge(edges) // 合并新添加的链接和现有的链接
        // .style('stroke-width', d => d.extendsProb);

    edges.exit().remove(); // 移除不再需要的链接

    // Update positions each tick
    simulation.nodes(filteredPapers).alpha(1).restart();
    simulation.force('link').links(filteredLinks);
    simulation.alphaTarget(0.3).restart();

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
