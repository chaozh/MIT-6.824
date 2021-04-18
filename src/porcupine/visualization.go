package porcupine

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
)

type historyElement struct {
	ClientId    int
	Start       int64
	End         int64
	Description string
}

type linearizationStep struct {
	Index            int
	StateDescription string
}

type partialLinearization = []linearizationStep

type partitionVisualizationData struct {
	History               []historyElement
	PartialLinearizations []partialLinearization
	Largest               map[int]int
}

type visualizationData = []partitionVisualizationData

func computeVisualizationData(model Model, info linearizationInfo) visualizationData {
	model = fillDefault(model)
	data := make(visualizationData, len(info.history))
	for partition := 0; partition < len(info.history); partition++ {
		// history
		n := len(info.history[partition]) / 2
		history := make([]historyElement, n)
		callValue := make(map[int]interface{})
		returnValue := make(map[int]interface{})
		for _, elem := range info.history[partition] {
			switch elem.kind {
			case callEntry:
				history[elem.id].ClientId = elem.clientId
				history[elem.id].Start = elem.time
				callValue[elem.id] = elem.value
			case returnEntry:
				history[elem.id].End = elem.time
				history[elem.id].Description = model.DescribeOperation(callValue[elem.id], elem.value)
				returnValue[elem.id] = elem.value
			}
		}
		// partial linearizations
		largestIndex := make(map[int]int)
		largestSize := make(map[int]int)
		linearizations := make([]partialLinearization, len(info.partialLinearizations[partition]))
		partials := info.partialLinearizations[partition]
		sort.Slice(partials, func(i, j int) bool {
			return len(partials[i]) > len(partials[j])
		})
		for i, partial := range partials {
			linearization := make(partialLinearization, len(partial))
			state := model.Init()
			for j, histId := range partial {
				var ok bool
				ok, state = model.Step(state, callValue[histId], returnValue[histId])
				if ok != true {
					panic("valid partial linearization returned non-ok result from model step")
				}
				stateDesc := model.DescribeState(state)
				linearization[j] = linearizationStep{histId, stateDesc}
				if largestSize[histId] < len(partial) {
					largestSize[histId] = len(partial)
					largestIndex[histId] = i
				}
			}
			linearizations[i] = linearization
		}
		data[partition] = partitionVisualizationData{
			History:               history,
			PartialLinearizations: linearizations,
			Largest:               largestIndex,
		}
	}
	return data
}

func Visualize(model Model, info linearizationInfo, output io.Writer) error {
	data := computeVisualizationData(model, info)
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(output, html, jsonData)
	if err != nil {
		return err
	}
	return nil
}

func VisualizePath(model Model, info linearizationInfo, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return Visualize(model, info, f)
}

const html = `
<!DOCTYPE html>
<html>
  <head><title>Porcupine</title>
    <style>
html {
  font-family: Helvetica, Arial, sans-serif;
  font-size: 16px;
}

text {
  dominant-baseline: middle;
}

#legend {
  position: fixed;
  left: 10px;
  top: 10px;
  background-color: rgba(255, 255, 255, 0.5);
  backdrop-filter: blur(3px);
  padding: 5px 2px 1px 2px;
  border-radius: 4px;
}

#canvas {
  margin-top: 45px;
}

#calc {
  width: 0;
  height: 0;
  visibility: hidden;
}

.bg {
  fill: transparent;
}

.divider {
  stroke: #ccc;
  stroke-width: 1;
}

.history-rect {
  stroke: #888;
  stroke-width: 1;
  fill: #42d1f5;
}

.link {
  fill: #206475;
  cursor: pointer;
}

.selected {
  stroke-width: 5;
}

.target-rect {
  opacity: 0;
}

.history-text {
  font-size: 0.9rem;
  font-family: Menlo, Courier New, monospace;
}

.hidden {
  opacity: 0.2;
}

.hidden line {
  opacity: 0.5; /* note: this is multiplicative */
}

.linearization {
  stroke: rgba(0, 0, 0, 0.5);
}

.linearization-invalid {
  stroke: rgba(255, 0, 0, 0.5);
}

.linearization-point {
  stroke-width: 5;
}

.linearization-line {
  stroke-width: 2;
}

.tooltip {
  position: absolute;
  opacity: 0;
  border: 1px solid #ccc;
  background: white;
  border-radius: 4px;
  padding: 5px;
  font-size: 0.8rem;
}

.inactive {
  display: none;
}
    </style>
  </head>
  <body>
    <div id="legend">
      <svg xmlns="http://www.w3.org/2000/svg" width="660" height="20">
        <text x="0" y="10">Clients</text>
        <line x1="50" y1="0" x2="70" y2="20" stroke="#000" stroke-width="1"></line>
        <text x="70" y="10">Time</text>
        <line x1="110" y1="10" x2="200" y2="10" stroke="#000" stroke-width="2"></line>
        <polygon points="200,5 200,15, 210,10" fill="#000"></polygon>
        <rect x="300" y="5" width="10" height="10" fill="rgba(0, 0, 0, 0.5)"></rect>
        <text x="315" y="10">Valid LP</text>
        <rect x="400" y="5" width="10" height="10" fill="rgba(255, 0, 0, 0.5)"></rect>
        <text x="415" y="10">Invalid LP</text>
        <text x="520" y="10" id="jump-link" class="link">[ jump to first error ]</text>
      </svg>
    </div>
    <div id="canvas">
    </div>
    <div id="calc">
    </div>
    <script>
      'use strict'

      const SVG_NS = 'http://www.w3.org/2000/svg'

      function svgnew(tag, attrs) {
        const el = document.createElementNS(SVG_NS, tag)
        svgattr(el, attrs)
        return el
      }

      function svgattr(el, attrs) {
        if (attrs != null) {
          for (var k in attrs) {
            if (Object.prototype.hasOwnProperty.call(attrs, k)) {
              el.setAttributeNS(null, k, attrs[k])
            }
          }
        }
      }

      function svgattach(parent, child) {
        return parent.appendChild(child)
      }

      function svgadd(el, tag, attrs) {
        return svgattach(el, svgnew(tag, attrs))
      }

      function newArray(n, fn) {
        const arr = new Array(n)
        for (let i = 0; i < n; i++) {
          arr[i] = fn(i)
        }
        return arr
      }

      function arrayEq(a, b) {
        if (a === b) {
          return true
        }
        if (a == null || b == null) {
          return false
        }
        if (a.length != b.length) {
          return false
        }
        for (let i = 0; i < a.length; i++) {
          if (a[i] !== b[i]) {
            return false
          }
        }
        return true
      }

      function render(data) {
        const PADDING = 10
        const BOX_HEIGHT = 30
        const BOX_SPACE = 15
        const XOFF = 20
        const EPSILON = 20
        const LINE_BLEED = 5
        const BOX_GAP = 20
        const BOX_TEXT_PADDING = 10
        const HISTORY_RECT_RADIUS = 4

        let maxClient = -1
        data.forEach(partition => {
          partition['History'].forEach(el => {
            maxClient = Math.max(maxClient, el['ClientId'])
          })
        })
        const nClient = maxClient + 1

        // Prepare some useful data to be used later:
        // - Add a GID to each event
        // - Create a mapping from GIDs back to events
        // - Create a set of all timestamps
        // - Create a set of all start timestamps
        const allTimestamps = new Set()
        const startTimestamps = new Set()
        let gid = 0
        const byGid = {}
        data.forEach(partition => {
          partition['History'].forEach(el => {
            allTimestamps.add(el['Start'])
            startTimestamps.add(el['Start'])
            allTimestamps.add(el['End'])
            // give elements GIDs
            el['Gid'] = gid
            byGid[gid] = el
            gid++
          })
        })
        let sortedTimestamps = Array.from(allTimestamps).sort((a, b) => a - b)

        // This should not happen with "real" histories, but for certain edge
        // cases, we need to deal with having multiple events share a start/end
        // time. We solve this by tweaking the events that share the end time,
        // updating the time to end+epsilon. In practice, rather than having to
        // choose an epsilon, we choose to average the value with the next largest
        // timestamp.
        const nextTs = {}
        for (let i = 0; i < sortedTimestamps.length-1; i++) {
          nextTs[sortedTimestamps[i]] = sortedTimestamps[i+1]
        }
        data.forEach(partition => {
          partition['History'].forEach(el => {
            let end = el['End']
            el['OriginalEnd'] = end // for display purposes
            if (startTimestamps.has(end)) {
              if (Object.prototype.hasOwnProperty.call(nextTs, end)) {
                const tweaked = (end + nextTs[end])/2
                el['End'] = tweaked
                allTimestamps.add(tweaked)
              }
            }
          })
        })
        // Update sortedTimestamps, because we created some new timestamps.
        sortedTimestamps = Array.from(allTimestamps).sort((a, b) => a - b)

        // Compute layout.
        //
        // We warp time to make it easier to see what's going on. We can think
        // of there being a monotonically increasing mapping from timestamps to
        // x-positions. This mapping should satisfy some criteria to make the
        // visualization interpretable:
        //
        // - distinguishability: there should be some minimum distance between
        // unequal timestamps
        // - visible text: history boxes should be wide enough to fit the text
        // they contain
        // - enough space for LPs: history boxes should be wide enough to fit
        // all linearization points that go through them, while maintaining
        // readability of linearizations (where each LP in a sequence is spaced
        // some minimum distance away from the previous one)
        //
        // Originally, I thought about this as a linear program:
        //
        // - variables for every unique timestamp, x_i = warp(timestamp_i)
        // - objective: minimize sum x_i
        // - constraint: non-negative
        // - constraint: ordering + distinguishability, timestamp_i < timestamp_j -> x_i + EPS < x_j
        // - constraint: visible text, size_text_j < x_{timestamp_j_end} - x_{timestamp_j_start}
        // - constraint: linearization lines have points that fit within box, ...
        //
        // This used to actually be implemented using an LP solver (without the
        // linearization point part, though that should be doable too), but
        // then I realized it's possible to solve optimally using a greedy
        // left-to-right scan in linear time.
        //
        // So that is what we do here. We optimally solve the above, and while
        // doing so, also compute some useful information (e.g. x-positions of
        // linearization points) that is useful later.
        const xPos = {}
        // Compute some information about history elements, sorted by end time;
        // the most important information here is box width.
        const byEnd = data.flatMap(partition =>
          partition['History'].map(el => {
            // compute width of the text inside the history element by actually
            // drawing it (in a hidden div)
            const scratch = document.getElementById('calc')
            scratch.innerHTML = ''
            const svg = svgadd(scratch, 'svg')
            const text = svgadd(svg, 'text', {
              'text-anchor': 'middle',
              'class': 'history-text',
            })
            text.textContent = el['Description']
            const bbox = text.getBBox()
            const width = bbox.width + 2*BOX_TEXT_PADDING
            return {
              'start': el['Start'],
              'end': el['End'],
              'width': width,
              'gid': el['Gid']
            }
          })
        ).sort((a, b) => a.end - b.end)
        // Some preprocessing for linearization points and illegal next
        // linearizations. We need to figure out where exactly LPs end up
        // as we go, so we can make sure event boxes are wide enough.
        const eventToLinearizations = newArray(gid, () => []) // event -> [{index, position}]
        const eventIllegalLast = newArray(gid, () => []) // event -> [index]
        const allLinearizations = []
        let lgid = 0
        data.forEach(partition => {
          partition['PartialLinearizations'].forEach(lin => {
            const globalized = [] // linearization with global indexes instead of partition-local ones
            const included = new Set() // for figuring out illegal next LPs
            lin.forEach((id, position) => {
              included.add(id['Index'])
              const gid = partition['History'][id['Index']]['Gid']
              globalized.push(gid)
              eventToLinearizations[gid].push({'index': lgid, 'position': position})
            })
            allLinearizations.push(globalized)
            let minEnd = Infinity
            partition['History'].forEach((el, index) => {
              if (!included.has(index)) {
                minEnd = Math.min(minEnd, el['End'])
              }
            })
            partition['History'].forEach((el, index) => {
              if (!included.has(index) && el['Start'] < minEnd) {
                eventIllegalLast[el['Gid']].push(lgid)
              }
            })
            lgid++
          })
        })
        const linearizationPositions = newArray(lgid, () => []) // [[xpos]]
        // Okay, now we're ready to do the left-to-right scan.
        // Solve timestamp -> xPos.
        let eventIndex = 0
        xPos[sortedTimestamps[0]] = 0 // positions start at 0
        for (let i = 1; i < sortedTimestamps.length; i++) {
          // left-to-right scan, finding minimum time we can use
          const ts = sortedTimestamps[i]
          // ensure some gap from last timestamp
          let pos = xPos[sortedTimestamps[i-1]] + BOX_GAP
          // ensure that text fits in boxes
          while (eventIndex < byEnd.length && byEnd[eventIndex].end <= ts) {
            // push our position as far as necessary to accommodate text in box
            const event = byEnd[eventIndex]
            const textEndPos = xPos[event.start] + event.width
            pos = Math.max(pos, textEndPos)
            // Ensure that LPs fit in box.
            //
            // When placing the end of an event, for all partial linearizations
            // that include that event, for the prefix that comes before that event,
            // all their start points must have been placed already, so we can figure
            // out the minimum width that the box needs to be to accommodate the LP.
            eventToLinearizations[event.gid]
              .concat(eventIllegalLast[event.gid].map(index => {
                return {
                  'index': index,
                  'position': allLinearizations[index].length-1,
                }
              }))
              .forEach(li => {
                const {index, position} = li
                for (let i = linearizationPositions[index].length; i <= position; i++) {
                  // determine past points
                  let prev = null
                  if (linearizationPositions[index].length != 0) {
                    prev = linearizationPositions[index][i-1]
                  }
                  const nextGid = allLinearizations[index][i]
                  let nextPos
                  if (prev === null) {
                    nextPos = xPos[byGid[nextGid]['Start']]
                  } else {
                    nextPos = Math.max(xPos[byGid[nextGid]['Start']], prev + EPSILON)
                  }
                  linearizationPositions[index].push(nextPos)
                }
                // this next line only really makes sense for the ones in
                // eventToLinearizations, not the ones from eventIllegalLast,
                // but it's safe to do it for all points, so we don't bother to
                // distinguish.
                pos = Math.max(pos, linearizationPositions[index][position])
              })
            // ensure that illegal next LPs fit in box too
            eventIllegalLast[event.gid].forEach(li => {
              const lin = linearizationPositions[li]
              const prev = lin[lin.length-1]
              pos = Math.max(pos, prev + EPSILON)
            })

            eventIndex++
          }
          xPos[ts] = pos
        }

        // Solved, now draw UI.

        let selected = false
        let selectedIndex = [-1, -1]

        const height = 2*PADDING + BOX_HEIGHT * nClient + BOX_SPACE * (nClient - 1)
        const width = 2*PADDING + XOFF + xPos[sortedTimestamps[sortedTimestamps.length-1]]
        const svg = svgadd(document.getElementById('canvas'), 'svg', {
          'width': width,
          'height': height,
        })

        // draw background, etc.
        const bg = svgadd(svg, 'g')
        const bgRect = svgadd(bg, 'rect', {
          'height': height,
          'width': width,
          'x': 0,
          'y': 0,
          'class': 'bg',
        })
        bgRect.onclick = handleBgClick
        for (let i = 0; i < nClient; i++) {
          const text = svgadd(bg, 'text', {
            'x': XOFF/2,
            'y': PADDING + BOX_HEIGHT/2 + i * (BOX_HEIGHT + BOX_SPACE),
            'text-anchor': 'middle',
          })
          text.textContent = i
        }
        svgadd(bg, 'line', {
          'x1': PADDING + XOFF,
          'y1': PADDING,
          'x2': PADDING + XOFF,
          'y2': height - PADDING,
          'class': 'divider'
        })

        // draw history
        const historyLayers = []
        const historyRects = []
        const targetRects = svgnew('g')
        data.forEach((partition, partitionIndex) => {
          const l = svgadd(svg, 'g')
          historyLayers.push(l)
          const rects = []
          partition['History'].forEach((el, elIndex) => {
            const g = svgadd(l, 'g')
            const rx = xPos[el['Start']]
            const width = xPos[el['End']] - rx
            const x = rx + XOFF + PADDING
            const y = PADDING + el['ClientId'] * (BOX_HEIGHT + BOX_SPACE)
            rects.push(svgadd(g, 'rect', {
              'height': BOX_HEIGHT,
              'width': width,
              'x': x,
              'y': y,
              'rx': HISTORY_RECT_RADIUS,
              'ry': HISTORY_RECT_RADIUS,
              'class': 'history-rect'
            }))
            const text = svgadd(g, 'text', {
              'x': x + width/2,
              'y': y + BOX_HEIGHT/2,
              'text-anchor': 'middle',
              'class': 'history-text',
            })
            text.textContent = el['Description']
            // we don't add mouseTarget to g, but to targetRects, because we
            // want to layer this on top of everything at the end; otherwise, the
            // LPs and lines will be over the target, which will create holes
            // where hover etc. won't work
            const mouseTarget = svgadd(targetRects, 'rect', {
              'height': BOX_HEIGHT,
              'width': width,
              'x': x,
              'y': y,
              'class': 'target-rect',
              'data-partition': partitionIndex,
              'data-index': elIndex,
            })
            mouseTarget.onmouseover = handleMouseOver
            mouseTarget.onmousemove = handleMouseMove
            mouseTarget.onmouseout = handleMouseOut
            mouseTarget.onclick = handleClick
          })
          historyRects.push(rects)
        })

        // draw partial linearizations
        const illegalLast = data.map(partition => {
          return partition['PartialLinearizations'].map(() => new Set())
        })
        const largestIllegal = data.map(() => {return {}})
        const largestIllegalLength = data.map(() => {return {}})
        const partialLayers = []
        const errorPoints = []
        data.forEach((partition, partitionIndex) => {
          const l = []
          partialLayers.push(l)
          partition['PartialLinearizations'].forEach((lin, linIndex) => {
            const g = svgadd(svg, 'g')
            l.push(g)
            let prevX = null
            let prevY = null
            let prevEl = null
            const included = new Set()
            lin.forEach(id => {
              const el = partition['History'][id['Index']]
              const hereX = PADDING + XOFF + xPos[el['Start']]
              const x = prevX !== null ? Math.max(hereX, prevX + EPSILON) : hereX
              const y = PADDING + el['ClientId'] * (BOX_HEIGHT + BOX_SPACE) - LINE_BLEED
              // line from previous
              if (prevEl !== null) {
                svgadd(g, 'line', {
                  'x1': prevX,
                  'x2': x,
                  'y1': prevEl['ClientId'] >= el['ClientId'] ? prevY : prevY + BOX_HEIGHT + 2*LINE_BLEED,
                  'y2': prevEl['ClientId'] <= el['ClientId'] ? y : y + BOX_HEIGHT + 2*LINE_BLEED,
                  'class': 'linearization linearization-line',
                })
              }
              // current line
              svgadd(g, 'line', {
                'x1': x,
                'x2': x,
                'y1': y,
                'y2': y + BOX_HEIGHT + 2*LINE_BLEED,
                'class': 'linearization linearization-point'
              })
              prevX = x
              prevY = y
              prevEl = el
              included.add(id['Index'])
            })
            // show possible but illegal next linearizations
            // a history element is a possible next try
            // if no other history element must be linearized earlier
            // i.e. forall others, this.start < other.end
            let minEnd = Infinity
            partition['History'].forEach((el, index) => {
              if (!included.has(index)) {
                minEnd = Math.min(minEnd, el['End'])
              }
            })
            partition['History'].forEach((el, index) => {
              if (!included.has(index) && el['Start'] < minEnd) {
                const hereX = PADDING + XOFF + xPos[el['Start']]
                const x = prevX !== null ? Math.max(hereX, prevX + EPSILON) : hereX
                const y = PADDING + el['ClientId'] * (BOX_HEIGHT + BOX_SPACE) - LINE_BLEED
                // line from previous
                svgadd(g, 'line', {
                  'x1': prevX,
                  'x2': x,
                  'y1': prevEl['ClientId'] >= el['ClientId'] ? prevY : prevY + BOX_HEIGHT + 2*LINE_BLEED,
                  'y2': prevEl['ClientId'] <= el['ClientId'] ? y : y + BOX_HEIGHT + 2*LINE_BLEED,
                  'class': 'linearization-invalid linearization-line',
                })
                // current line
                const point = svgadd(g, 'line', {
                  'x1': x,
                  'x2': x,
                  'y1': y,
                  'y2': y + BOX_HEIGHT + 2*LINE_BLEED,
                  'class': 'linearization-invalid linearization-point',
                })
                errorPoints.push({
                  x: x,
                  partition: partitionIndex,
                  index: lin[lin.length-1]['Index'], // NOTE not index
                  element: point
                })
                illegalLast[partitionIndex][linIndex].add(index)
                if (!Object.prototype.hasOwnProperty.call(largestIllegalLength[partitionIndex], index) || largestIllegalLength[partitionIndex][index] < lin.length) {
                  largestIllegalLength[partitionIndex][index] = lin.length
                  largestIllegal[partitionIndex][index] = linIndex
                }
              }
            })
          })
        })
        errorPoints.sort((a, b) => a.x - b.x)

        // attach targetRects
        svgattach(svg, targetRects)

        // tooltip
        const tooltip = document.getElementById('canvas').appendChild(document.createElement('div'))
        tooltip.setAttribute('class', 'tooltip')

        function handleMouseOver() {
          if (!selected) {
            const partition = parseInt(this.dataset['partition'])
            const index = parseInt(this.dataset['index'])
            highlight(partition, index)
          }
          tooltip.style.opacity = 1
        }

        function linearizationIndex(partition, index) {
          // show this linearization
          if (Object.prototype.hasOwnProperty.call(data[partition]['Largest'], index)) {
            return data[partition]['Largest'][index]
          } else if (Object.prototype.hasOwnProperty.call(largestIllegal[partition], index)) {
            return largestIllegal[partition][index]
          }
          return null
        }

        function highlight(partition, index) {
          // hide all but this partition
          historyLayers.forEach((layer, i) => {
            if (i === partition) {
              layer.classList.remove('hidden')
            } else {
              layer.classList.add('hidden')
            }
          })
          // hide all but the relevant linearization
          partialLayers.forEach(layer => {
            layer.forEach(g => {
              g.classList.add('hidden')
            })
          })
          // show this linearization
          const maxIndex = linearizationIndex(partition, index)
          if (maxIndex !== null) {
            partialLayers[partition][maxIndex].classList.remove('hidden')
          }
          updateJump()
        }

        let lastTooltip = [null, null, null, null, null]
        function handleMouseMove() {
          const partition = parseInt(this.dataset['partition'])
          const index = parseInt(this.dataset['index'])
          const [sPartition, sIndex] = selectedIndex
          const thisTooltip = [partition, index, selected, sPartition, sIndex]

          if (!arrayEq(lastTooltip, thisTooltip)) {
            let maxIndex
            if (!selected) {
              maxIndex = linearizationIndex(partition, index)
            } else {
              // if selected, show info relevant to the selected linearization
              maxIndex = linearizationIndex(sPartition, sIndex)
            }
            if (selected && sPartition !== partition) {
              tooltip.innerHTML = 'Not part of selected partition.'
            } else if (maxIndex === null) {
              if (!selected) {
                tooltip.innerHTML = 'Not part of any partial linearization.'
              } else {
                tooltip.innerHTML = 'Selected element is not part of any partial linearization.'
              }
            } else {
              const lin = data[partition]['PartialLinearizations'][maxIndex]
              let prev = null, curr = null
              let found = false
              for (let i = 0; i < lin.length; i++) {
                prev = curr
                curr = lin[i]
                if (curr['Index'] === index) {
                  found = true
                  break
                }
              }
              let call = data[partition]['History'][index]['Start']
              let ret = data[partition]['History'][index]['OriginalEnd']
              let msg = ''
              if (found) {
                // part of linearization
                if (prev !== null) {
                  msg = '<strong>Previous state:</strong><br>' + prev['StateDescription'] + '<br><br>'
                }
                msg += '<strong>New state:</strong><br>' + curr['StateDescription'] +
                  '<br><br>Call: ' + call +
                  '<br><br>Return: ' + ret
              } else if (illegalLast[partition][maxIndex].has(index)) {
                // illegal next one
                msg = '<strong>Previous state:</strong><br>' + lin[lin.length-1]['StateDescription'] +
                  '<br><br><strong>New state:</strong><br>&langle;invalid op&rangle;' +
                  '<br><br>Call: ' + call +
                  '<br><br>Return: ' + ret
              } else {
                // not part of this one
                msg = 'Not part of selected element\'s partial linearization.'
              }
              tooltip.innerHTML = msg
            }
            lastTooltip = thisTooltip
          }
          tooltip.style.left = (event.pageX+20) + 'px'
          tooltip.style.top = (event.pageY+20) + 'px'
        }

        function handleMouseOut() {
          if (!selected) {
            resetHighlight()
          }
          tooltip.style.opacity = 0
          lastTooltip = [null, null, null, null, null]
        }

        function resetHighlight() {
          // show all layers
          historyLayers.forEach(layer => {
            layer.classList.remove('hidden')
          })
          // show longest linearizations, which are first
          partialLayers.forEach(layers => {
            layers.forEach((l, i) => {
              if (i === 0) {
                l.classList.remove('hidden')
              } else {
                l.classList.add('hidden')
              }
            })
          })
          updateJump()
        }

        function updateJump() {
          const jump = document.getElementById('jump-link')
          // find first non-hidden point
          // feels a little hacky, but it works
          const point = errorPoints.find(pt => !pt.element.parentElement.classList.contains('hidden'))
          if (point) {
            jump.classList.remove('inactive')
            jump.onclick = () => {
              point.element.scrollIntoView({behavior: 'smooth', inline: 'center', block: 'center'})
              if (!selected) {
                select(point.partition, point.index)
              }
            }
          } else {
            jump.classList.add('inactive')
          }
        }

        function handleClick() {
          const partition = parseInt(this.dataset['partition'])
          const index = parseInt(this.dataset['index'])
          if (selected) {
            const [sPartition, sIndex] = selectedIndex
            if (partition === sPartition && index === sIndex) {
              deselect()
              return
            } else {
              historyRects[sPartition][sIndex].classList.remove('selected')
            }
          }
          select(partition, index)
        }

        function handleBgClick() {
          deselect()
        }

        function select(partition, index) {
          selected = true
          selectedIndex = [partition, index]
          highlight(partition, index)
          historyRects[partition][index].classList.add('selected')
        }

        function deselect() {
          if (!selected) {
            return
          }
          selected = false
          resetHighlight()
          const [partition, index] = selectedIndex
          historyRects[partition][index].classList.remove('selected')
        }

        handleMouseOut() // initialize, same as mouse out
      }

      const data = %s

      render(data)
    </script>
  </body>
</html>
`
