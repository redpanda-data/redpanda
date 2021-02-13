import React, { useState, useEffect } from "react";

// Images
import pandaFace from "assets/images/logos/panda-face-2.svg";

const Logo = ({ count }: { count: number }) => {
  return (
    <img src={pandaFace} height="38px" alt="Logo" style={{ display: "block" }} />
  );
};

export default Logo;
