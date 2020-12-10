import React, { useState, useEffect } from "react";

// Images
import pandaFace from "assets/images/logos/panda-face-2.svg";
import pandaFaceBlink from "assets/images/logos/panda-face-2-blink.svg";

const Logo = ({ count }: { count: number }) => {
  const [image, setImage] = useState(pandaFace);

  useEffect(() => {
    setImage(pandaFaceBlink);
    setTimeout(() => setImage(pandaFace), 400);
  }, [count]);

  return (
    <img src={image} height="38px" alt="Logo" style={{ display: "block" }} />
  );
};

export default Logo;
