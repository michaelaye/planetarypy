--[[
Wrap known acronyms in <abbr title="...">ACRONYM</abbr> for hover tooltips.

Edit the `abbr` table below to add entries. The filter walks every Str
inline in the document and substitutes any token that matches a key,
preserving surrounding punctuation. Only the *first* match per node is
wrapped to keep DOM noise down — subsequent occurrences in the same Str
are left as plain text (browsers usually pick the first <abbr> for the
tooltip anyway, and over-wrapping clutters HTML).

Active only for HTML output; LaTeX/PDF pipelines skip the filter.
]]--

local abbr = {
  NSSDC   = "National Space Science Data Center (NASA Goddard)",
  GSFC    = "Goddard Space Flight Center",
  NAIF    = "Navigation and Ancillary Information Facility (NASA JPL)",
  PCK     = "Planetary Constants Kernel (NAIF SPICE kernel format)",
  SPICE   = "Spacecraft, Planet, Instrument, C-matrix, Events — NAIF toolkit",
  IAU     = "International Astronomical Union",
  GM      = "Standard gravitational parameter (G times mass)",
  CDX     = "Capture Index (Wayback Machine query API)",
  DOI     = "Digital Object Identifier",
  PDS     = "Planetary Data System (NASA)",
  CODATA  = "Committee on Data of the International Science Council",
  JPL     = "Jet Propulsion Laboratory (NASA / Caltech)",
  MOLA    = "Mars Orbiter Laser Altimeter",
  HiRISE  = "High Resolution Imaging Science Experiment (MRO)",
  MRO     = "Mars Reconnaissance Orbiter",
  CTX     = "Context Camera (MRO)",
  EDR     = "Experimental Data Record (PDS Level-0 data product)",
  CRS     = "Coordinate Reference System",
}

if FORMAT ~= "html" and FORMAT ~= "html4" and FORMAT ~= "html5" then
  return {}
end

local function wrap(token, defn)
  -- aria-label (not title) so the native browser tooltip doesn't fire
  -- alongside the CSS-styled one. Screen readers still read the
  -- expansion via aria-label.
  return pandoc.RawInline("html",
    '<abbr aria-label="' .. defn:gsub('"', "&quot;")
    .. '" class="acro">' .. token .. '</abbr>')
end

function Str(el)
  local text = el.text
  for token, defn in pairs(abbr) do
    -- Match the token surrounded only by word-boundary-safe chars
    local prefix, suffix = text:match("^(.-)(" .. token .. ")(.*)$")
    if prefix and suffix ~= nil then
      local before = prefix
      local after  = text:sub(#prefix + #token + 1)
      -- Boundary check: char before/after token should be non-alphanumeric
      local lc = before:sub(-1)
      local rc = after:sub(1, 1)
      if (lc == "" or not lc:match("[%w]"))
         and (rc == "" or not rc:match("[%w]")) then
        local parts = {}
        if #before > 0 then table.insert(parts, pandoc.Str(before)) end
        table.insert(parts, wrap(token, defn))
        if #after > 0 then table.insert(parts, pandoc.Str(after)) end
        return parts
      end
    end
  end
  return nil
end
